# 0033 — The website moves out of the tree, and its rules go with it

- **Status:** accepted
- **Date:** 2026-09-26
- **Accepted:** 2026-10-13
- **Items:** closes B-254; retires what B-253 built
- **Supersedes:** [0025](0025-the-website.md), all of it
- **Related:** [0025](0025-the-website.md) §2, §4.1-§4.7, §5; [0028](0028-trace-propagation-is-the-callers.md)
  (B-246's numbers, re-measured); `Cargo.toml` (`homepage`, `repository`)

## 1. The question

[0025](0025-the-website.md) put the website in this tree as `crates/site`, a workspace member
that rendered `README.md` and `docs/` with `pulldown-cmark` and wrote no prose of its own. It
was built for a state that no longer holds: the owner's two constraints then were that the site
lives here and that there is no public release, and it was parked (B-254) because a site is a
promise of something to install.

Both constraints changed. There is a release: `0.1.0-alpha.1` and `0.1.0-alpha.2` of `weida`,
`weida-core`, `weida-protocol`, `weida-runtime` and `weida-winpipe` are on crates.io, and
`0.1.0-alpha.2` of `weida-broker`, `weida-raft` and `weida-openbao` (checked against
`https://crates.io/api/v1/crates/<name>` on 2026-09-26; the other seventeen members answer
404). That list grew while this note was written, which is §4.4's argument in miniature. And
the owner decided on 2026-09-26 that `weida.doodleshnookie.net` is a **separate
Zola site in its own repository**, beside the hub (`blog.doodleshnookie.net`) and the zeughaus
site (`zeughaus.doodleshnookie.net`), which already share that toolchain.

So the question is not whether the site moves. It is which of 0025's rules survive the move,
where each one now lives, and what the tree loses by no longer rendering its own documents.

## 2. What 0025 was protecting, and whether it depends on the crate

0025 §2 is the argument and it does not depend on where the renderer lives: **every coherence
sweep this project has run found its defects in prose, not code**, and a landing page carrying
its own summary of the guarantees would be the first thing to go stale. What protected the
document set was never `crates/site` as such but four properties of it:

1. the documents are imported, not rewritten (§4.1);
2. every document is published or excluded with its reason, and one that is neither fails
   (§4.2);
3. a link to an excluded document goes to the repository, and a link to nothing fails (§4.3);
4. the site says what state the project is in (§4.4).

Each of these is a property of the **build step that reads the documents**, and a build step
can live anywhere that has the documents on disk at build time. The fifth thing the crate gave
— being run by `cargo test --workspace` — is the only one that is a property of the crate's
location, and it is the loss §4.8 names.

## 3. Options

| | what it is | named loss |
| --- | --- | --- |
| A — keep `crates/site` and deploy it | serve `target/site` as it is | the owner wants a designed landing page in the family of the hub and the zeughaus site; growing templates, a type system and a layout inside a Rust crate is a second site generator this project would own alone |
| B — a Zola site with the documents copied into it | the usual project site | a second copy of every claim, the defect of 0025 §2 adopted on purpose |
| C — **a Zola site that imports the documents at build time** | a sync script reads this tree, rewrites links, writes Zola content; the landing page is the only page written there | the site leaves `cargo test --workspace`; a change here that breaks the site is caught by the site's build, not by this tree's gate |
| D — keep `crates/site` as the renderer and wrap its HTML in the Zola site | two renderers, one inside the other | two Markdown pipelines and two templates for one page, and every rule of 0025 split across two repositories |

## 4. Decision

**C.** The site is built in its own repository by importing this one; this tree keeps the
documents and nothing that renders them. Each rule of 0025 maps as follows.

1. **§4.1, the site writes no prose of its own, survives in a stronger form.** Normative text is
   never rewritten: the sync script (`sync/import.py` in the site repository) copies every
   published document's body as it stands and changes only its links, its code-fence info
   strings and the place of its first heading, which becomes the page title, so every sentence
   on a reference page is a sentence in `docs/` or `README.md`. The
   landing page is the **only designed page**, and what it states about weida it quotes: the
   lead and the sections of `README.md`, and the crate map from `cargo metadata` and the
   README's crates table. 0025 made `README.md` the landing page; the landing page now *reads*
   `README.md`, so a claim on it still has one source, and that source is here.

2. **§4.2, published or excluded, moves into the sync script** with the same exclusion list and
   the same reasons: the loop's bookkeeping (`BACKLOG.md`, `NIGHTLOG.md`, `LOOP.md`), a named
   third party's requirements (`docs/requirements/`), the research sheets condensed from other
   projects' specifications (`docs/research/`), and the master plan. A document under `docs/` or
   a root `*.md` that is neither mapped nor excluded makes the script exit non-zero naming it,
   which is the property `every_document_is_published_or_excluded_on_purpose` asserted.

3. **§4.3, links, moves with it.** A link to an excluded document becomes a link into the
   public repository (`https://github.com/tuco86/weida`, the `repository` of `Cargo.toml`); a
   relative link that resolves to nothing in this tree makes the script exit non-zero and lists
   every such link. The site build is still a link checker for this document set.

4. **§4.4, the pre-release notice, is replaced by the release state read from crates.io at
   build time.** The notice said "no crate on any registry", which stopped being true with
   `0.1.0-alpha.1`. A sentence that is true on the day it is written and false on the day of the
   next publish is the drift of 0025 §2 again, so the site does not carry one: the sync script
   asks crates.io, per member of `cargo metadata`, whether it is published and at which version,
   and the site states that. What is not on crates.io is stated as not published.

5. **§4.5, no web font and no JavaScript, is half kept.** The site uses **self-hosted web
   fonts** and **no JavaScript**. 0025 said a web font would need a reason; the reason is that
   the site is now a designed page in a family of designed pages, and self-hosting keeps the
   part of the rule that mattered — a visitor's browser talks to no third party. The `file://`
   property goes: the site is served, and read locally through Zola's own server.

6. **§4.6, nothing deploys it, is reversed.** The site repository builds and deploys the site;
   this tree contains no publication step, and B-254 closes with this note.

7. **§4.7, the same Markdown dialect as `rustdoc`, survives.** Zola renders with
   `pulldown-cmark` too, so a document read on the site and a doc comment read on docs.rs are
   still one dialect. The workspace dependency on `pulldown-cmark` goes with the crate that was
   its only user.

8. **The named loss: the site is no longer in `cargo test --workspace`.** 0025 §5 counted it as
   the property that kept the site honest, and a change in this tree can now break the site
   without failing this tree's gate. The sync script is the gate on the site side: it fails on
   an unaccounted document, a dangling link, and a crate map that no longer matches
   `cargo metadata`, and the site is not built past a failure. What that does not give is the
   timing — a broken link is found when the site is next built, not when the commit is made.

`crates/site` is removed, and with it `weida_site::NOTICE`, the package `weida-site` and the
`cargo run -p weida-site` instructions in `README.md`.

## 5. Consequences and follow-ups

- **0025 is superseded, not rewritten.** Its argument (§2) stands and is cited by
  [GUIDE.md](../GUIDE.md) and [0026](0026-the-guide-and-the-c8b-question.md); its mechanism is
  this note's.
- **The member count moved again**: `cargo metadata --no-deps` lists **25** members after the
  removal, and the README's crates table lists all of them, including `weida-openbao`, which
  [0032](0032-identity-sources-and-the-handoff.md) added and the table had not.
- **The two preconditions 0025 §5 set before the site could be public are met**: a release
  form exists (crates.io), and B-246's overhead numbers were re-measured in
  [0028](0028-trace-propagation-is-the-callers.md).
- **The public repository must exist before the site links to it.** Every link to an excluded
  document now points at `https://github.com/tuco86/weida`; on 2026-09-26 that address answers
  404, so this repository is pushed there before the site goes up.
- **What the site must never gain** is unchanged from 0025: a page whose claims are not
  checkable in this tree.
