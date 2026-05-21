# Incident Response Plan

How we respond to security vulnerabilities. For reporting, see [SECURITY.md](../../SECURITY.md).

## Quick Reference

| Phase | Timeline | Action |
|-------|----------|--------|
| **Triage** | ≤ 7 days | Reproduce, assess severity, create private advisory |
| **Mitigation** | 1-30 days | Fix quietly, test, review |
| **Disclosure** | Same day as patch | Release patch + publish advisory simultaneously |
| **Learning** | 2 weeks | Debrief, document root cause, prevent recurrence |

---

## ⚠️ Important: Self-Hosted Deployment Model

Unlike centralized platforms, security patches for Respira are distributed through GitHub code updates. Each operator must:

1. Pull the latest code from GitHub or checkout the release tag
2. Rebuild their Docker container
3. Deploy to their instance

This means patch adoption is **staggered**—not all instances update simultaneously.

**For operators:** Monitor GitHub Releases and Security Advisories for critical patches. Subscribe to notifications in the repository settings.

**For maintainers:** Use clear, urgent language in release notes for critical vulnerabilities.

---

## Phase 1: Triage (≤ 7 days)

1. **Read & acknowledge:**
   - Read the report carefully (twice if complex)
   - Reply within 3 business days: "Thanks for reporting. We're investigating. Update in 3 business days."
   - Copy key details to reference as you investigate

2. **Reproduce:**
   - Clone/pull latest code
   - Follow exact steps from report
   - Try to trigger the vulnerability
   - Document: Does it reproduce? On which versions? Which endpoint/feature?

3. **Assess severity:**
   - **High:** Can be exploited immediately, significant impact (data access, code execution, auth bypass)
   - **Medium:** Requires specific conditions, notable impact (limited users, specific configuration)
   - **Low:** Difficult to exploit or minimal impact (edge case, information leak only)

4. **Create private advisory:**
   - Go to GitHub repo > Security and Quality tab > Report a Vulnerability
   - Or: Advisories > New advisory (to create privately)
   - Fill: Title, description, affected versions
   - Click "Save as draft" (do NOT publish yet)
   - Copy the advisory link

5. **Notify team:**
   - Send advisory link to core maintainers
   - Include: severity, brief summary, fix owner name, target completion date
   - Example: "High severity - SQL injection in search. Owner: Jane. Target: March 30."

**Key decision:** Is this actually a vulnerability, or user misconfiguration?

---

## Phase 2: Mitigation (1-30 days)

**Before starting, determine timeline based on severity (from Phase 1).**

1. **Create a private fix:**
   - Create new local branch (don't push to GitHub yet)
   - Write code to patch the vulnerability
   - Never commit exploit details or step-by-step attack instructions

2. **Test thoroughly:**
   - Reproduce original vulnerability in dev environment
   - Apply your fix
   - Verify the vulnerability is resolved
   - Run existing tests to check for regressions
   - Test edge cases and related features

3. **Code review:**
   - At least one other maintainer reviews the code
   - Check: Does it solve the problem? Any security gaps? Any unintended side effects?
   - Use review checklist if available

4. **Build & verify:**
   - Ensure build succeeds with no new errors
   - Run full test suite
   - Verify deployment artifacts are clean

5. **Prepare for release (do NOT release yet):**
   - Document what changed
   - Prepare release notes / patch summary
   - Keep embargo until disclosure date

**Timeline depends on severity: High (7-14 days), Medium (14-30 days), Low (30+ days)**

---

## Phase 3: Disclosure (Same day: release AND publish advisory)

Release and disclosure happen together. Do NOT publish advisory before patch is available.

1. **Release the patch:**
   - Push code to GitHub (main/release branch)
   - Create a new release tag (v1.2.3, etc.)
   - Add changelog/release notes documenting the fix
   - Users will pull the latest code and rebuild their containers

2. **Publish the advisory:**
   - Go to draft advisory > Click "Publish"
   - Advisory includes:
     - **Title:** Vulnerability name (e.g., "SQL Injection in Search API")
     - **Description:** What the vulnerability is, in plain language
     - **Affected versions:** Which versions are vulnerable?
     - **Impact:** What can attacker do? (read data, execute code, etc.)
     - **Upgrade instructions:** How to get the fix
       - Example: "Pull latest code from GitHub and rebuild your container"
       - Or: "Checkout release tag: `git checkout v1.2.3`"
     - **CVE:** If you have one, include the ID
   - Do NOT include: step-by-step exploit, exact payload, vulnerable code snippet

3. **Guide users to upgrade:**
   - GitHub advisory auto-notifies watchers
   - Link to advisory from README or docs if critical
   - Update changelogs or deployment guides
   - Send email/notification to known major downstream projects if applicable
   - Note: Operators are responsible for pulling the code and rebuilding their containers
   - Provide step-by-step upgrade instructions in release notes
   - Include clear guidance: "Pull latest code: `git pull && git checkout vX.Y.Z`"

4. **Credit the reporter:**
   - Ask first: "May we credit you in the advisory?"
   - Default: Credit them unless they ask for anonymity
   - Never share their personal contact info publicly

---

## Phase 4: Patch Distribution & Operator Communication

Once the advisory is published, operators are responsible for pulling the patch. Unlike SaaS platforms where all users auto-receive updates, Respira uses a distributed model:

1. **Release timing:** Ensure patch code is pushed to GitHub and release tags are created BEFORE advisory is published

2. **Release notes:** Include:
    - Affected versions (e.g., "Affects all versions up to v1.2.3")
    - Upgrade urgency (e.g., "CRITICAL: Security vulnerability")
    - Upgrade path: `git pull && git checkout v1.2.4` or `git pull && docker compose build`
    - Backward compatibility notes (if any breaking changes)

3. **Operator actions:** Each operator independently:
    - Receives notification (GitHub Releases watch, Security Advisory notification)
    - Decides when to update (may be delayed for various reasons)
    - Pulls code and rebuilds container
    - Redeploys to their environment

4. **Staggered adoption:** Some operators may take weeks to patch. Critical vulnerabilities should be communicated with escalation:
    - Use "CRITICAL" in release notes
    - Consider pinning notice to README
    - Reemphasize in subsequent releases if critical unpatched

---

## Phase 5: Post-Incident Learning (Within 2 weeks)

1. **Debrief with the team:**
   - Why did this vulnerability exist?
   - Was it a code review gap? Testing gap? Architecture gap?
   - How did we catch it (reporter, scanner, exploit)?
   - Were response timelines realistic?
   - What was slow or painful?

2. **Prevent recurrence:**
   - **Add a test case:** Write test that reproduces original bug (prevents regression)
   - **Update code review:** Add checklist item if needed (e.g., "Validate all user inputs")
   - **Improve documentation:** Clarify secure usage patterns if applicable
   - **Consider tooling:** Do we need new linting, scanning, or security checks?

3. **Update this plan:**
   - If we learned something new, update this document
   - Record lessons in team wiki or post-incident summary

4. **Optional: Public summary:**
   - Blog post or GitHub Discussion explaining what happened and what changed
   - Helps community learn from the incident

---

## First 15 Minutes Checklist

When vulnerability report arrives, follow this checklist. Takes ~15 minutes total.

```
MIN 1-2: Read & Extract
 [ ] What's the vulnerability?
 [ ] Which versions affected?
 [ ] How to reproduce?
 [ ] What's the impact?
 [ ] Severity guess?

MIN 3-4: Create Private Advisory
 [ ] GitHub > Security and Quality tab > Advisories > New
 [ ] Fill title + description
 [ ] Click "Save as draft" (keep private)
 [ ] Copy advisory link

MIN 5-6: Acknowledge Reporter
 [ ] Reply: "Thanks for reporting. We're investigating.
     Update in 3 business days."

MIN 7-8: Notify Team
 [ ] Send advisory link to core maintainers
 [ ] Include severity + assigned fix owner

MIN 9-12: Try to Reproduce
 [ ] Clone latest code
 [ ] Follow exact steps from report
 [ ] Confirm it's real or ask for clarification

MIN 13-14: Assign & Update
 [ ] Update advisory with findings (reproduced? which versions?)
 [ ] Assign fix owner name
 [ ] Set target completion date

RESULT:
 - Reporter acknowledged
 - Team notified
 - Fix assigned + dated
 - Vulnerability reproduced
```

---

## Severity & Response Time

| Severity | Acknowledge | Assess | Fix Target |
|----------|---|---|---|
| **High** | 3 days | 7 days | 7-14 days |
| **Medium** | 3 days | 7 days | 14-30 days |
| **Low** | 3 days | 7 days | 30+ days |

Times are targets. For complex issues, communicate with reporter explaining any delays.

---

## Roles

- **Incident Commander:** Coordinates response, owns timeline
- **Developer:** Writes fix
- **Reviewer:** Code review before release
- **Communicator:** Drafts advisory, manages announcements

One person can fill multiple roles.
The incident commander is often the developer + reviewer + communicator. The critical checkpoint is always code review (ideally by another maintainer, but even a second pair of eyes helps).

---

## Communication

- **Report:** GitHub Security Advisory form
- **Internal coordination:** Private GitHub Security Advisory (draft)
- **Public announcement:** Published GitHub Security Advisory

---

## What NOT to Do

- Do NOT discuss in public issues before patch exists
- Do NOT publish exploit details or step-by-step attack instructions
- Do NOT delay acknowledgment (reply within 3 days)
- Do NOT publish advisory before patch is available
- Do NOT skip code review before release

---

## What If Your Team Is Unavailable?

For critical vulnerabilities discovered when no one is available:

1. **Escalate immediately:** Reach out to team members directly

2. **Use interim measures:** If you can't fix in 24-48 hours:
    - Draft advisory and save as private (don't publish until patch exists)

---

See [SECURITY.md](../../SECURITY.md) for reporting.
