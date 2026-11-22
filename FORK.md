# kudzuweb/t3code

This is Mauria's fork of `pingdotgg/t3code`. It adds a way to resume Claude Code, Codex and Pi sessions started in a terminal from inside T3, and it keeps the two views of a session in step. Everything else is upstream.

## Differences from upstream

| Change                                                                                                       | Where                                                          | Drop when               |
| ------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------- | ----------------------- |
| The carried fix from upstream PR #38528 makes a Claude turn see messages added to its session in a terminal. | `apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts` | Upstream merges #38528. |

## Syncing with upstream

Run `git fetch upstream`, `git checkout main`, `git merge upstream/main`, then push `main` to `origin`. The fork's `main` is published, so it is merged and never rebased. When a carried upstream fix lands upstream, the merge will usually resolve it, and its row in the table above is then removed.

## Pull requests

Fork PRs are opened with `gh pr create --repo kudzuweb/t3code --base main`, because `gh` run in a fork otherwise targets upstream.
