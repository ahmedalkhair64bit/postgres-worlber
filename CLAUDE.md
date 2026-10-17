## gstack (recommended)

This project uses [gstack](https://github.com/garrytan/gstack) for AI-assisted
workflows. Install it once per machine (team mode keeps it updated):

```bash
git clone --single-branch --depth 1 https://github.com/garrytan/gstack.git ~/.claude/skills/gstack
cd ~/.claude/skills/gstack && ./setup --team
```

Then restart your AI coding tool.

Use the `/browse` skill from gstack for all web browsing. Never use
`mcp__claude-in-chrome__*` tools.

Available gstack skills:

- /office-hours
- /plan-ceo-review
- /plan-eng-review
- /plan-design-review
- /design-consultation
- /design-shotgun
- /design-html
- /review
- /deslop-shared-libs
- /test-audit
- /ship
- /eval-plan
- /land-and-deploy
- /canary
- /benchmark
- /browse
- /connect-chrome
- /qa
- /qa-only
- /design-review
- /scrape
- /setup-browser-cookies
- /setup-deploy
- /setup-gbrain
- /retro
- /investigate
- /document-release
- /document-generate
- /codex
- /cso
- /autoplan
- /plan-devex-review
- /devex-review
- /careful
- /freeze
- /guard
- /unfreeze
- /gstack-upgrade
- /learn
