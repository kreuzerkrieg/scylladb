#!/usr/bin/env bash
# Driver for the S3 throttling fleet (SCYLLADB-3386).
#
#   ./s3-fleet.sh launch  <count>     spin up N spot instances, tagged for teardown
#   ./s3-fleet.sh setup               RAID-0 + deps + binary + creds on every instance
#   ./s3-fleet.sh run     [args...]   start the test on all of them, one node prefix each
#   ./s3-fleet.sh collect             pull RESULT lines and logs
#   ./s3-fleet.sh kill                terminate the whole fleet
#
# Deliberately not a single command: setup is slow and worth keeping warm across runs.
set -uo pipefail

export AWS_PROFILE=797456418907-DevOpsAccessRole
export AWS_DEFAULT_REGION=us-east-1

FLEET="${FLEET:-ernest-s3fleet-$(date +%F)}"
KEY=ernest
PEM=~/.ssh/ernest.pem
SG=sg-01ab61b39746e1bb6
# Fedora 44 x86_64. The release must match the build host (`cat /etc/fedora-release`),
# because the binary links that toolchain's libraries -- remote-setup.sh upgrades
# libstdc++/libgcc to cover the point-release drift, but not a whole release apart.
# If this AMI is ever deregistered, look up a current one (Fedora publishes under
# AWS account 125523088429):
#
#   aws ec2 describe-images --owners 125523088429 \
#     --filters "Name=name,Values=Fedora-Cloud-Base-*" "Name=architecture,Values=x86_64" \
#     --query 'reverse(sort_by(Images,&CreationDate))[:5].[ImageId,Name,CreationDate]' \
#     --output text
#
# An aarch64 fleet needs an aarch64 build of the binary as well as an arm64 AMI.
AMI="${AMI:-ami-049fafedd823e029b}"
# Overridable: on another machine the clone lives elsewhere. Must be a RELEASE
# build -- a dev build links seastar dynamically and dies on the instance.
BIN="${BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)/build/release/test/perf/perf_s3_downloader}"
STAGE_BUCKET=manager-backup-tests-us-east-1
SRC_BUCKET=${SRC_BUCKET:-manager-backup-tests-permanent-snapshots-us-east-1}
SRC_PREFIX=${SRC_PREFIX:-ernest-sct-tests/6TB-tablets-RF3-6node}
UPLOAD_BUCKET=${UPLOAD_BUCKET:-manager-backup-tests-us-east-1}
UPLOAD_PREFIX=${UPLOAD_PREFIX:-sstables_ewz}
# Key-layout knobs, all opt-in so the default stays the X1 layout:
#   RANDOM_PREFIX=<n>  prepend n random base64url chars directly under the bucket
#   NO_PREFIX=1        drop UPLOAD_PREFIX from the key (boost rejects an empty value,
#                      hence a switch rather than --upload_prefix "")
#   HASH_PREFIX=1      PR 30846's murmur-hash element ahead of the sstable_id
RANDOM_PREFIX=${RANDOM_PREFIX:-0}
NO_PREFIX=${NO_PREFIX:-}
# Resolve the newest CLion generation: the path is pinned per major version and
# JetBrains copies scratches on upgrade, so a hardcoded one silently writes into
# an abandoned directory.
# Where collect() drops the per-node logs. The CLion scratches path is this
# workstation's convention; anywhere else it falls back to the repo-local dir.
OUT="${OUT:-$(ls -dt ~/.config/JetBrains/CLion*/ 2>/dev/null | head -1)scratches/GitHubCopilot/_internal/fleet-runs}"
[[ "$OUT" == scratches/* ]] && OUT="$(dirname "${BASH_SOURCE[0]}")/fleet-runs"

# The six per-node directories, one per instance.
NODES=(
  d19d62b8-1c89-11f1-b388-025bfb20f2b9
  d1a065bc-1c89-11f1-92b7-02c21ce218c3
  d1ba103e-1c89-11f1-9eab-027eb0300c8b
  d211556a-1c89-11f1-b1f0-02d6605209f7
  d2868786-1c89-11f1-a55c-02a2fd76cc55
  d3518738-1c89-11f1-8d72-02b6db3c724f
)

SSH="ssh -i $PEM -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15"
SCP="scp -i $PEM -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

ips() {
  aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=$FLEET" "Name=instance-state-name,Values=running" \
    --query 'Reservations[].Instances[].PublicIpAddress' --output text | tr '\t' '\n' | grep -v '^$'
}

# Enumerate every AZ that can actually host this fleet, cheapest spot first.
#
# Hand-maintaining a candidate list meant AZs were silently missing -- us-east-1e
# was never tried because no subnet there had ever been established -- and the
# list was ordered by nothing in particular, so a partial fill could land on
# expensive capacity while cheaper capacity sat unqueried one entry later.
#
# TYPES may name several instance types; every type is paired with every AZ.
# Only subnets that auto-assign a public IP qualify, because the whole driver is
# ssh-based: an instance without one is worse than a missing instance, since it
# bills while being unreachable. Where an AZ has several, take the one with the
# most free addresses.
discover_candidates() {
  local types=(${TYPES:-i4i.8xlarge i7i.8xlarge})

  # Restrict to the security group's own VPC. Without this filter describe-subnets
  # returns public subnets from every VPC in the account, and since the pick below
  # is "most free addresses wins", it lands on a foreign VPC wherever that VPC's
  # subnet happens to be emptier. RunInstances then fails with InvalidParameter
  # ("security group and subnet belong to different networks"), which reads as a
  # capacity shortage in the launch log. On 2026-07-30 that silently disqualified
  # 5 of 6 AZs and cost a full launch cycle.
  local vpc
  vpc=$(aws ec2 describe-security-groups --group-ids "$SG" \
          --query 'SecurityGroups[0].VpcId' --output text 2>/dev/null)
  [[ -n "$vpc" && "$vpc" != None ]] || { echo "cannot resolve VPC for $SG" >&2; return 1; }

  declare -A subnet_of=()
  local az subnet free
  while read -r az subnet free; do
    [[ -n "$az" && -n "$subnet" ]] || continue
    [[ -n "${subnet_of[$az]:-}" ]] || subnet_of[$az]=$subnet
  done < <(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$vpc" \
             --query 'Subnets[?MapPublicIpOnLaunch==`true`].[AvailabilityZone,SubnetId,AvailableIpAddressCount]' \
             --output text 2>/dev/null | sort -k1,1 -k3,3nr)

  # Price is per (type, az) and moves, so read it live rather than caching it.
  # An AZ with no recent quote still gets tried, last, at a sentinel price: a
  # missing quote is not evidence of missing capacity.
  # An AZ that does not offer the type at all is not a capacity problem and must not
  # occupy a candidate slot: us-east-1e carries no i4i/i7i, so every request there
  # returns Unsupported no matter how long you retry.
  local t p offered
  for t in "${types[@]}"; do
    offered=$(aws ec2 describe-instance-type-offerings --location-type availability-zone \
                --filters "Name=instance-type,Values=$t" \
                --query 'InstanceTypeOfferings[].Location' --output text 2>/dev/null \
                | tr '\t\n' '  ')
    for az in "${!subnet_of[@]}"; do
      [[ " $offered " == *" $az "* ]] || continue
      p=$(aws ec2 describe-spot-price-history --instance-types "$t" --availability-zone "$az" \
            --product-descriptions Linux/UNIX --start-time "$(date -u -d '-2 hours' +%FT%TZ)" \
            --query 'SpotPriceHistory[0].SpotPrice' --output text 2>/dev/null)
      [[ "$p" == "None" || -z "$p" ]] && p=99
      printf '%s %s:%s:%s\n' "$p" "$t" "$az" "${subnet_of[$az]}"
    done
  done | sort -g | awk '{print $2}'
}

launch() {
  local want=$1 got=0
  # Spot capacity in the cheapest AZ is routinely unavailable, so walk candidates
  # cheapest-first across family AND az together and take what we can get.
  local cands
  if [[ -n "${CANDS:-}" ]]; then
    read -ra cands <<<"$CANDS"
  else
    mapfile -t cands < <(discover_candidates)
    (( ${#cands[@]} )) || { echo "no candidates discovered"; return 1; }
    echo "candidates (cheapest spot first):"
    printf '    %s\n' "${cands[@]}"
  fi
  # Always try spot first -- it is 2-3x cheaper -- and fall back to on-demand only
  # once spot has been refused in every candidate AZ. Never skip spot outright:
  # even when placement scores are poor, some AZ usually has room, and a spot fleet
  # that does get reclaimed costs less than running the whole thing on-demand.
  local try_markets=(spot ondemand)
  [[ "${MARKET:-}" == "ondemand" ]] && try_markets=(ondemand)   # force, for reproducibility runs
  # Market on the OUTSIDE, candidates on the inside. With the loops the other way
  # round, spot was tried only in the first candidate's AZ: on-demand there would
  # satisfy the whole count and the loop would break before spot was ever tried in
  # the remaining AZs. That is not hypothetical -- it happened on 2026-07-30, when
  # spot in us-east-1f was $1.80 against $5.49 on-demand and was never asked.
  for mkt in "${try_markets[@]}"; do
    (( got >= want )) && break
    for c in "${cands[@]}"; do
      (( got >= want )) && break
      IFS=: read -r type az subnet <<<"$c"
      local need=$(( want - got )) market=()
      [[ "$mkt" == spot ]] && market=(--instance-market-options 'MarketType=spot')
      echo "--> requesting $need x $type in $az ($mkt)"
      local ids
      # --count 1:$need rather than --count $need: a bare count sets min=max, so EC2 refuses
      # the whole request unless one AZ can supply the entire remaining count at once.
      # That reported InsufficientInstanceCapacity in all 12 type x AZ combinations on
      # 2026-07-30 for 16 x 8xlarge while capacity for smaller groups plainly existed.
      # Take whatever each AZ can give and let the loop accumulate towards $want.
      ids=$(aws ec2 run-instances --image-id "$AMI" --instance-type "$type" \
              --count "1:$need" \
              --key-name "$KEY" --security-group-ids "$SG" --subnet-id "$subnet" \
              "${market[@]}" \
              --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$FLEET},{Key=owner,Value=ernest},{Key=market,Value=$mkt}]" \
              --query 'Instances[].InstanceId' --output text 2>&1)
      if [[ $? -eq 0 ]]; then
        local n; n=$(wc -w <<<"$ids")
        got=$(( got + n )); echo "    got $n $mkt ($ids)"
      else
        # Distinguish the failure classes: an expired token, a permissions problem
        # and a genuine capacity shortage all land here, and reporting them all as
        # "no capacity" sends you hunting for capacity that was never the problem.
        local reason
        reason=$(grep -oE 'InsufficientInstanceCapacity|RequestExpired|ExpiredToken|UnauthorizedOperation''|RequestLimitExceeded|VcpuLimitExceeded|InstanceLimitExceeded''|MaxSpotInstanceCountExceeded|Unsupported|InvalidParameter[A-Za-z]*|Unknown options''|[A-Za-z]+Error' <<<"$ids" | head -1)
        case "$reason" in
          RequestExpired|ExpiredToken)
            echo "    CREDENTIALS EXPIRED -- refresh and retry (not a capacity problem)"
            return 1 ;;
          UnauthorizedOperation)
            echo "    NOT AUTHORIZED to run instances -- check the role"
            return 1 ;;
          InsufficientInstanceCapacity|"")
            echo "    no $mkt capacity${reason:+: $reason}" ;;
          *)
            echo "    $mkt request failed: $reason" ;;
        esac
      fi
    done
  done
  echo "launched $got/$want; waiting for running..."
  aws ec2 wait instance-running --filters "Name=tag:Name,Values=$FLEET" 2>/dev/null
  ips | nl
}

setup() {
  # Stage the binary once; each instance pulls it in-region. Uploading 554 MB per
  # instance over the local uplink is otherwise the slowest part of the whole run.
  echo "staging binary..."
  aws s3 cp "$BIN" "s3://$STAGE_BUCKET/spot-fleet/perf_s3_downloader" --only-show-errors
  local url; url=$(aws s3 presign "s3://$STAGE_BUCKET/spot-fleet/perf_s3_downloader" --expires-in 7200)

  # Credentials go via a generated script: the session token contains +/= and
  # cannot be passed inline through ssh.
  SRC_BUCKET="$SRC_BUCKET" python3 - <<'PY'
import configparser, os, shlex
c = configparser.ConfigParser(); c.read(os.path.expanduser('~/.aws/credentials'))
p = '797456418907-DevOpsAccessRole'
# Quoted heredoc: nothing here is expanded by the shell, so "$@" survives
# verbatim into the generated script and the args actually reach the binary.
open('/tmp/fleet_run.sh', 'w').write(f"""#!/bin/bash
export AWS_ACCESS_KEY_ID={shlex.quote(c.get(p, 'aws_access_key_id'))}
export AWS_SECRET_ACCESS_KEY={shlex.quote(c.get(p, 'aws_secret_access_key'))}
export AWS_SESSION_TOKEN={shlex.quote(c.get(p, 'aws_session_token'))}
export AWS_DEFAULT_REGION=us-east-1
export S3_SERVER_ADDRESS_FOR_TEST=s3.us-east-1.amazonaws.com
export S3_BUCKET_FOR_TEST={os.environ['SRC_BUCKET']}
ulimit -n 524288
cd /mnt/data
exec ./perf_s3_downloader "$@"
""")
PY

  local rs; rs="$(dirname "$0")/remote-setup.sh"
  [[ -f "$rs" ]] || { echo "missing $rs"; return 1; }

  : > /tmp/fleet_setup_failed
  local pids=()
  for ip in $(ips); do
    ( $SCP "$rs" fedora@"$ip":/tmp/remote-setup.sh >/dev/null 2>&1
      out=$($SSH fedora@"$ip" "chmod +x /tmp/remote-setup.sh && /tmp/remote-setup.sh '$url'" 2>&1)
      # remote-setup.sh prints SETUP_OK only when the instance is actually
      # usable. Previously "ready" was echoed unconditionally, so six silently
      # broken instances still looked fine and the failure only surfaced later.
      if grep -q SETUP_OK <<<"$out"; then
        echo "[$ip] ready  $(grep '^df:' <<<"$out")"
        $SCP /tmp/fleet_run.sh fedora@"$ip":/mnt/data/run.sh >/dev/null 2>&1
        $SSH fedora@"$ip" 'chmod +x /mnt/data/run.sh' 2>/dev/null
      else
        echo "[$ip] SETUP FAILED"
        sed 's/^/    /' <<<"$out" | tail -5
        echo "$ip" >> /tmp/fleet_setup_failed
      fi ) &
    pids+=($!)
  done
  # Bare `wait` waits for every child of this shell, which includes the
  # watchdog's multi-minute sleep -- that blocked the run from ever starting.
  wait "${pids[@]}"
  rm -f /tmp/fleet_run.sh
  if [[ -s /tmp/fleet_setup_failed ]]; then
    echo "setup failed on $(wc -l < /tmp/fleet_setup_failed) instance(s) -- aborting"
    return 1
  fi
}

# Both phases in ONE screen session, chained with &&. This matters beyond
# convenience: it makes "no process running" mean "all work finished". Running the
# phases as separate invocations leaves a gap after the download where the box
# looks idle but is not done, and anything that reaps idle fleets will destroy the
# corpus in exactly that window -- which is how 19 TB was lost on 2026-07-27.
# PHASES=download   only the restore-shaped download (default; threshold hunting)
# PHASES=both       download, then upload the corpus, chained in one session
# UPLOAD_PASSES=N   upload the same corpus N times (default 1). Each pass mints
#                   fresh sstable_ids so passes accumulate rather than overwrite,
#                   and the corpus is read-only so it survives. This lengthens the
#                   upload window without lengthening the download: throttling
#                   onset was measured at 240-360s while a 20-min download yields
#                   only a ~310s upload, so a single pass can end before S3 pushes
#                   back and read as a false negative.
# ROUND_MIN=0       run until the slice is exhausted; otherwise a fixed round
run() {
  local hosts=(); mapfile -t hosts < <(ips)
  local n=${#hosts[@]}
  (( n )) || { echo "no running instances"; return 1; }
  local phases="${PHASES:-download}" round="${ROUND_MIN:-0}"

  # Assemble the upload key-layout arguments once so the remote command line stays
  # readable and every combination is expressible from the environment.
  local upload_args="--upload_bucket $UPLOAD_BUCKET"
  if [[ -n "$NO_PREFIX" ]]; then
    upload_args="$upload_args --upload_no_prefix"
  else
    upload_args="$upload_args --upload_prefix $UPLOAD_PREFIX"
  fi
  (( RANDOM_PREFIX > 0 )) && upload_args="$upload_args --upload_random_prefix $RANDOM_PREFIX"
  [[ -n "${HASH_PREFIX:-}" ]] && upload_args="$upload_args --upload_hash_prefix"
  echo "upload args: $upload_args"
  echo "starting $n instances: phases=$phases round=${round}min fleet_size=$n"

  # Every instance takes the slice of the dataset whose position in the ordered
  # listing is congruent to its index. This replaces the old one-node-prefix-per-box
  # scheme, which silently broke past six instances: ${NODES[$i]} ran out and boxes
  # 7+ were handed an empty prefix, i.e. the whole dataset, overlapping each other.
  local i=0
  local pids=()
  for ip in "${hosts[@]}"; do
    ( local dl="./run.sh --default-log-level info --logger-log-level default_http_retry_strategy=debug \
          --mode download --prefix $SRC_PREFIX/ \
          --fleet_size $n --fleet_index $i \
          --round_timeout $round --max_rounds 1 --sstable_concurrency 16 --sample_interval 60 \
          --corpus_dir /mnt/data/corpus $* > /mnt/data/dl.txt 2>&1"
      local cmd="$dl"
      if [[ "$phases" == both ]]; then
        # Chained in one session on purpose: it makes "no process running" mean
        # "all work finished". Run as separate invocations and the box looks idle
        # in the gap after the download, which is how 19 TB was destroyed.
        cmd="$dl"
        local pass
        for (( pass = 1; pass <= ${UPLOAD_PASSES:-1}; pass++ )); do
          local out=/mnt/data/ul.txt
          (( pass > 1 )) && out=/mnt/data/ul${pass}.txt
          # UPLOAD_EXTRA reaches the upload only; "$*" is forwarded to the download
          # alone, so raising upload concurrency was not expressible before.
          cmd="$cmd && ./run.sh --default-log-level info --logger-log-level default_http_retry_strategy=debug \
              --mode upload --corpus_dir /mnt/data/corpus $upload_args ${UPLOAD_EXTRA:-} \
              > $out 2>&1"
        done
      fi
      # Fan out in parallel and use setsid rather than screen. ssh does not
      # reliably return from a backgrounded remote command, so a sequential loop
      # started only two of six boxes -- seven minutes apart, which also ruins any
      # concurrent measurement. A screen session is no help either: a watcher
      # started inside one dies with its parent's process group.
      $SSH -n fedora@"$ip" "cd /mnt/data && rm -rf corpus dl.txt ul.txt && mkdir -p corpus && \
        setsid nohup bash -c \"$cmd\" >/dev/null 2>&1 </dev/null & exit" >/dev/null 2>&1
      sleep 3
      local procs
      procs=$($SSH -n fedora@"$ip" 'pgrep -cf "[p]erf_s3_downloader"' 2>/dev/null)
      echo "[$ip] slice $i/$n procs=${procs:-0}" ) &
    pids+=($!)
    i=$((i+1))
  done
  wait "${pids[@]}"

  sleep 25
  local bad=0
  for ip in "${hosts[@]}"; do
    if ! $SSH -n fedora@"$ip" "pgrep -f '[p]erf_s3_downloader' >/dev/null" 2>/dev/null; then
      echo "[$ip] NOT RUNNING after start -- $($SSH -n fedora@"$ip" 'head -3 /mnt/data/dl.txt' 2>/dev/null)"
      bad=$((bad+1))
    fi
  done
  (( bad )) && echo "WARNING: $bad/$n instances are not running"
  echo "started; poll with: $0 collect"
}

collect() {
  mkdir -p "$OUT"
  local stamp; stamp=$(date +%H%M%S)
  for ip in $(ips); do
    local running; running=$($SSH fedora@"$ip" "pgrep -f '[p]erf_s3_downloader' >/dev/null && echo RUNNING || echo DONE" 2>/dev/null)
    echo "=== $ip [$running] ==="
    $SSH -n fedora@"$ip" 'grep -hoE "RESULT \{.*" /mnt/data/dl.txt /mnt/data/ul*.txt 2>/dev/null' 2>/dev/null
    # ul*.txt, not a fixed "dl ul" list: with UPLOAD_PASSES=2 the second pass is the
    # one that throttles hardest, and omitting ul2.txt meant the decisive pass was
    # never archived. On 2026-08-02 that made it impossible to say afterwards whether
    # two lost objects had been denied a retry by the budget or had exhausted it,
    # because the fleet was already terminated.
    local phases; phases=$($SSH -n fedora@"$ip" 'ls /mnt/data/dl.txt /mnt/data/ul*.txt 2>/dev/null' 2>/dev/null)
    local f
    for f in $phases; do
      $SCP fedora@"$ip":"$f" "$OUT/${stamp}-${ip}-$(basename "$f")" >/dev/null 2>&1
    done
  done
  echo "logs -> $OUT/${stamp}-*.txt"
}

kill_fleet() {
  local ids; ids=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=$FLEET" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[].Instances[].InstanceId' --output text)
  [[ -z "$ids" ]] && { echo "nothing to terminate"; return; }
  echo "terminating: $ids"
  aws ec2 terminate-instances --instance-ids $ids --query 'TerminatingInstances[].[InstanceId,CurrentState.Name]' --output text
}

# launch -> setup -> run -> wait -> collect -> terminate, with teardown on any exit
# path. Nothing should keep billing while results are being discussed.
# Detached watchdog: terminates the fleet by tag after a hard deadline no matter
# what happens to the driver. The driver's own EXIT trap is not enough -- if the
# controlling session dies the driver dies with it and the instances keep billing.
arm_watchdog() {
  local deadline_min=$1
  setsid nohup bash -c "
    sleep $(( deadline_min * 60 ))
    ids=\$(aws ec2 describe-instances \
      --filters 'Name=tag:Name,Values=$FLEET' 'Name=instance-state-name,Values=pending,running,stopping,stopped' \
      --query 'Reservations[].Instances[].InstanceId' --output text)
    [ -n \"\$ids\" ] && aws ec2 terminate-instances --instance-ids \$ids >/dev/null 2>&1
  " > /tmp/fleet-watchdog-$FLEET.log 2>&1 < /dev/null &
  disown   # keep it out of the job table so `wait` never blocks on it
  echo "watchdog armed: fleet $FLEET terminates unconditionally in ${deadline_min}m (pid $!)"
}

all() {
  local count=${1:-6} timeout_min=${2:-10}
  trap 'echo; echo "!! tearing down"; kill_fleet' EXIT INT TERM
  arm_watchdog $(( timeout_min + 25 ))
  launch "$count"
  if ! setup; then
    echo "aborting before the round: setup did not succeed everywhere"
    return 1
  fi
  run --round_timeout "$timeout_min"
  local deadline=$(( $(date +%s) + timeout_min*60 + 900 ))
  while (( $(date +%s) < deadline )); do
    sleep 60
    local left; left=$(for ip in $(ips); do
      $SSH fedora@"$ip" "pgrep -f '[p]erf_s3_downloader' >/dev/null && echo x" 2>/dev/null; done | wc -l)
    echo "  $(date +%H:%M:%S) still running: $left"
    (( left == 0 )) && break
  done
  collect
  trap - EXIT INT TERM
  kill_fleet
}

case "${1:-}" in
  all)     shift; all "$@" ;;
  launch)  shift; launch "${1:-6}" ;;
  setup)   setup ;;
  run)     shift; run "$@" ;;
  collect) collect ;;
  kill)    kill_fleet ;;
  ips)     ips ;;
  *) sed -n '2,12p' "$0" ;;
esac
