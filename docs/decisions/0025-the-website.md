# 0025 — The website is the document set, rendered

- **Status:** superseded by [0033](0033-the-website-moves-out.md)
- **Date:** 2026-09-14
- **Related:** B-068 (the publish metadata that named `homepage`); decisions 0013 §5.5, 0014 §2

## 1. The question

`Cargo.toml` has named `https://weida.doodleshnookie.net` as this workspace's `homepage`
since B-068, and nothing was there. The owner's instruction is narrow and it decides most of
this note: the site **lives in this tree**, and there is **no public release** — work
continues locally.

So the question is not "what should a project website say". It is: what can a site say
today without lying, where does the text come from, and what stops it from drifting away
from the repository the way a hand-written landing page always does.

## 2. What this repository already knows about drift

**Every coherence sweep this project has run found the same class of defect, and it was
never in the code.** The sweep of 2026-09-14 found fifteen incoherences and eleven of them
were counts and claims that had become false in prose: "five frame kinds" with six on the
wire, "three transports" with four implemented, `IncomingMeta` with "9 fields" and twelve in
the struct, a mapping document still refusing a pattern that had shipped, and a `README.md`
that said L2 "does not exist yet" with a broker in the tree. The review a few hours later
found a documented rule — "a consumer pauses by restating what it has already been
delivered" — that four places stated and the code could not do.

A marketing page is that defect with a bigger audience. A landing page carrying its own
summary of the guarantees would be the **first** thing to go stale, because it is the
furthest from the tests and the only page nobody re-reads while changing code.

**The documents are already the product.** `PROTOCOL.md` is normative and asserted by golden
vectors; `PATTERNS.md`'s tables name the tests that prove each row; `GUARANTEES.md` says what
is implemented rather than what is intended; `STATUS.md` is regenerated from measurements.
There is nothing a website could add to that set except navigation — and something that
claims to be more would be less.

**Two facts constrain what may appear anywhere.** There is no release: no crate on any
registry, no tag, no binary. And this repository's own rule for a claim is that it must be
checkable; "get started in five minutes" is not, while "the quick start below is a command in
this tree" is.

## 3. Options

| | what it is | named loss |
| --- | --- | --- |
| A — hand-written site beside the docs | HTML and prose written for the web, linking into the repository | a second copy of every claim, and the copy nobody re-reads. This is the drift class §2 names, adopted deliberately |
| B — an external generator (mdBook, Zola, Hugo) | a `book.toml` or a theme, the docs as chapters | a toolchain the gate cannot run: none of them is a Cargo dependency, so a site build would need a second install on every machine and could not be a test. mdBook would also impose its own navigation shape on a set that is not a book |
| C — **a workspace member that renders the documents** | one crate, one dependency, output to `target/site` | a Markdown dependency in the workspace, and a template that can only be as good as the documents it wraps |
| D — nothing yet; point the domain at the forge | zero work, honest | the `homepage` field promises a page; a redirect to a git browser is a worse answer than a rendered document set, and the repository is not public either |

## 4. Decision

**C, with the rule that makes it worth having: the site writes no prose of its own.**

1. **`crates/site` renders the repository's own documents and adds nothing but a shell.**
   Navigation, a stylesheet, a per-page heading index and one notice. Every sentence a
   visitor reads is a sentence in `docs/` or `README.md`, so there is no second copy to fall
   behind. The landing page **is** `README.md`.

2. **A document is published, or excluded with its reason written down.** The exclusions are
   three kinds and the distinction is the whole of the editorial policy: the loop's own
   bookkeeping (`BACKLOG.md`, `NIGHTLOG.md`, `LOOP.md`) is not documentation of a product; a
   named third party's requirements are theirs rather than ours to publish; and the research
   sheets under `docs/research/` are condensed from other projects' specifications and
   manuals, which are not ours to serve from a page that looks like ours. The master plan is
   excluded on a fourth ground — it is the brief this repository is built from, normative for
   the loop and a plan rather than a description of what exists.

   A document that is neither published nor excluded **fails a test**
   (`every_document_is_published_or_excluded_on_purpose`), so the site cannot quietly fall
   behind the directory it renders.

3. **A link to an excluded document becomes a link into the repository**, never a page that
   is not there. A link that resolves to nothing at all **fails the build**, which makes the
   site build a link checker for the document set — it found 58 broken links in the decision
   notes the first time it ran, all of them written root-relative from inside
   `docs/decisions/`.

4. **The notice is on every page**, not only on the landing page, because a direct link
   delivers a reader into the middle of the set: *"Pre-release. weida is not published: no
   crate on any registry, no tag, no binary. These documents are rendered from the repository
   and describe work in progress."* It lives in one place, `weida_site::NOTICE`, and a test
   asserts it on every rendered page.

5. **The output is files.** `cargo run -p weida-site` writes `target/site` and prints the
   landing page's path; every link between pages is relative, so the whole site works over
   `file://`. No server, no JavaScript, no web font, no analytics — the last three would each
   need a reason, and a document set has none.

6. **Nothing deploys it.** There is no publication step in this tree, no CI to run one
   (B-061 is blocked on a runner) and no release to announce. Serving it is one item, filed,
   and it needs the owner: which host serves the domain, and whether the site goes up before
   there is anything to install.

7. **One dependency, and the same one `rustdoc` uses.** `pulldown-cmark`, so a document
   rendered for the site and a doc comment rendered into the API documentation are the same
   Markdown dialect rather than two readings of one file. `default-features = false` drops
   the command-line parser its own binary needs; the crate is `publish = false`, so nothing
   reaches a library user's dependency graph.

## 5. Consequences and follow-ups

- **The site is in the gate.** `cargo test --workspace` renders the whole document set four
  times over and fails on a dangling link or an unaccounted document, which is the property
  that keeps this from becoming the thing §2 warns about.
- **The member count moved and the README was wrong about it.** Adding this crate made the
  count checkable again: `cargo metadata` lists **28** members and `README.md` claimed
  twenty-four while missing `weida-broker`, `weida-raft` and `weida-py` — a table that says
  it is "read off `cargo metadata`, not kept by hand" and was not. Fixed in the same pass.
- **B-253 is this note's implementation**; **B-254** is serving it, and stays `blocked` on
  the owner for the two questions above.
- **What the site must gain before it is public, and not before**: a quick start that names a
  release form (there is none yet, B-068's tag is unpushed), and the overhead numbers of
  B-246 re-measured, because the first thing a visitor compares is bytes on the wire.
- **What it must never gain**: a page whose claims are not checkable in this tree. If a
  sentence cannot live in a document that a test or a measurement backs, it does not belong
  on the site either.
