# Data preparation

[Github Archive](https://www.gharchive.org/)
[GitHub event types](https://docs.github.com/en/rest/using-the-rest-api/github-event-types)

<https://ghe.clickhouse.tech/>

```bash
GITHUB_EVENTS='2015-01-01-15.json'
DATA_DIR=./tests/data/

# extract users
jq -sc 'map(
    .payload.member,
    .payload.issue.user,
    .payload.issue.assigniee,
    .payload.issue.milestone.creator,
    .payload.comment.user,
    .payload.release.author,
    .payload.forkee.owner,
    .payload.pull_request.user,
    .payload.pull_request.head.user,
    .payload.pull_request.base.user,
    .payload.pull_request.merged_by
) | del(..|nulls) | unique_by(.id) | .[]' "${GITHUB_EVENTS}" > "${DATA_DIR}/users.json"

# extract repositories
jq -sc 'map(.repo) | del(..|nulls) | unique_by(.id) | .[]' "${GITHUB_EVENTS}" > "${DATA_DIR}/repositories.json"

# extract organizations
jq -sc 'map(.org) | del(..|nulls) | unique_by(.id) | .[]' "${GITHUB_EVENTS}" > "${DATA_DIR}/organizations.json"

# extract milestones
jq -sc 'map(.payload.issue.milestone) | del(..|nulls) | unique_by(.id) | map(
    .creator = .creator.id
) | .[]' "${GITHUB_EVENTS}" > "${DATA_DIR}/milestones.json"

# extract releases
jq -sc 'map(.payload.release) | del(..|nulls) | unique_by(.id) | map(
    if .author then .author = .author.id else . end
    | del(.assets)
) | .[]' "${GITHUB_EVENTS}" > "${DATA_DIR}/releases.json"

# extract issues
jq -sc 'map(.payload.issue) | del(..|nulls) | unique_by(.id) | map(
    .user = .user.id
    | .assignee  = .assignee.id
    | if .milestone then .milestone = .milestone.id else . end
    | del(.labels)
) | .[]' "${GITHUB_EVENTS}" > "${DATA_DIR}/issues.json"

# extract pull requests
jq -sc 'map(.payload.pull_request) | del(..|nulls) | unique_by(.id) | map(
    if .user then .user = .user.id else . end
    | if .assignee then .assignee = .assignee.id else . end
    | if .merged_by then .merged_by = .merged_by.id else . end
    | if .head.user then .head.user = .head.user.id else . end
    | if .head.repo then .head.repo = .head.repo.id else . end
    | if .base.user then .base.user = .base.user.id else . end
    | if .base.repo then .base.repo = .base.repo.id else . end
    | if .milestone then .milestone = .milestone.id else . end
    | del(._links)
) | .[]' "${GITHUB_EVENTS}" > "${DATA_DIR}/pull_requests.json"

# extract forks
jq -sc 'map(.payload.forkee) | del(..|nulls) | unique_by(.id) | map(
    .owner = .owner.id
) | .[]' "${GITHUB_EVENTS}" > "${DATA_DIR}/forks.json"

# extract comments
jq -sc 'map(.payload.comment) | del(..|nulls) | unique_by(.id) | map(
    .user = .user.id
    | del(._links)
) | .[]' "${GITHUB_EVENTS}" > "${DATA_DIR}/comments.json"

# normalize events
jq -sc 'map(
    .actor = .actor.id
    | .repo = .repo.id
    | if .org then .org = .org.id else . end
    | if .payload.pull_request then .payload.pull_request = .payload.pull_request.id else . end
    | if .payload.issue then .payload.issue = .payload.issue.id else . end
    | if .payload.comment then .payload.comment = .payload.comment.id else . end
    | if .payload.release then .payload.release = .payload.release.id else . end
    | if .payload.forkee then .payload.forkee = .payload.forkee.id else . end
    | if .payload.commits then .payload.commits = (.payload.commits | length) else . end
) | .[]' "${GITHUB_EVENTS}" > "${DATA_DIR}/events.json"
```


# Verifying

### Datafusion CLI

[Installation](https://datafusion.apache.org/user-guide/cli/installation.html)

```sql
CREATE EXTERNAL TABLE users
STORED AS JSON LOCATION 'tests/data/users.json';

CREATE EXTERNAL TABLE repositories
STORED AS JSON LOCATION 'tests/data/repositories.json';

CREATE EXTERNAL TABLE organizations
STORED AS JSON LOCATION 'tests/data/organizations.json';

CREATE EXTERNAL TABLE milestones
STORED AS JSON LOCATION 'tests/data/milestones.json';

CREATE EXTERNAL TABLE releases
STORED AS JSON LOCATION 'tests/data/releases.json';

CREATE EXTERNAL TABLE issues
STORED AS JSON LOCATION 'tests/data/issues.json';

CREATE EXTERNAL TABLE pull_requests
STORED AS JSON LOCATION 'tests/data/pull_requests.json';

CREATE EXTERNAL TABLE forks
STORED AS JSON LOCATION 'tests/data/forks.json';

CREATE EXTERNAL TABLE comments
STORED AS JSON LOCATION 'tests/data/comments.json';

CREATE EXTERNAL TABLE events
STORED AS JSON LOCATION 'tests/data/events.json';
```

### Python CLI

```python
import json
from pathlib import Path

def load_json(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(row) for row in f]


def load_tables():
    return {
        path.stem: load_json(path)
        for path in Path("tests/data/").glob("*.json")
    }

tables = load_tables()
```
