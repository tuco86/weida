# zeughaus-ci

weida is a repository of the zeughaus CI (`[repos.weida]` in the CI's
`ci.toml` on sadala) for its plans and proposals: `/zeughaus save` stores a
plan, the desktop's Plans tab approves it, and the master session executes
it. This folder marks the checkout as such for the zeughaus omp plugin.

It holds no job yet, so a push starts no pipeline. A job is a `.sh` or
`.ps1` file here with a `# /// ci` header; before the first one, the CI
needs a `status_token` and a `webhook_secret` for weida and the Forgejo hook
to `https://ci.doodleshnookie.net/hook/weida`.
