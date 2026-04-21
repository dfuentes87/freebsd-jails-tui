# Contributing

This document describes the process of contributing to this project. It is intended
for anyone considering opening an **issue**, **discussion** or **pull request**.

> [!NOTE]
>
> This is a personal project for fun and learning. Do not expect technical support
> with your system or personal assistance with usage of this tool. As with the
> BSD community, it is expected you learn on your own and read the documentation;
> in particular, the FreeBSD Jails documentation.

## The Critical Rule

**The most important rule: you must understand your code.** If you can't
explain what your changes do and how they interact with the greater system
without the aid of AI tools, do not contribute to this project.

Using AI to write code is fine. You can gain understanding by interrogating an
agent with access to the codebase until you grasp all edge cases and effects
of your changes. What's not fine is submitting agent-generated slop without
that understanding. Be sure to read the AI Usage Policy below.

## AI Usage Policy

This project has strict rules for AI usage:

- **All AI usage in any form must be disclosed.** You must state
  the tool you used (e.g. Claude Code, Cursor, Amp) along with
  the extent that the work was AI-assisted.

- **The human-in-the-loop must fully understand all code.** If you
  can't explain what your changes do and how they interact with the
  greater system without the aid of AI tools, do not contribute
  to this project.

- **Issues and discussions can use AI assistance but must have a full
  human-in-the-loop.** This means that any content generated with AI
  must have been reviewed _and edited_ by a human before submission.
  AI is very good at being overly verbose and including noise that
  distracts from the main point. Humans must do their research and
  trim this down.

- **No AI-generated media is allowed (art, images, videos, audio, etc.).**
  Text and code are the only acceptable AI-generated content, per the
  other rules in this policy.

- **Bad AI drivers will be denounced** People who produce bad contributions
  that are clearly AI slop will be added to the public denouncement list.
  This list will block all future contributions. Additionally, the list
  is public and may be used by other projects to be aware of bad actors.

These rules apply only to outside contributions to this project. Maintainers
are exempt from these rules and may use AI tools at their discretion;
they've proven themselves trustworthy to apply good judgment.

The reason for the strict AI policy is not due to an anti-AI stance, but
instead due to the number of highly unqualified people using AI.

## Denouncement System

If you repeatedly break the rules of this document or repeatedly
submit low quality work, you will be **denounced.** This adds your
username to a public list of bad actors who have wasted our time. All
future interactions on this project will be automatically closed by
bots.

The denouncement list is public, so other projects who trust our
maintainer judgement can also block you automatically.

## Quick Guide

### I'd like to contribute

All issues are actionable. Pick one and start
working on it. Thank you. If you need help or guidance, comment on the issue.

### I have a bug! / Something isn't working

First, search the issue tracker and discussions for similar issues. Tip: also
search for [closed issues] and [discussions] — your issue might have already
been fixed!

> [!NOTE]
>
> If there is an _open_ issue or discussion that matches your problem,
> **please do not comment on it unless you have valuable insight to add**.
>
> GitHub has a very _noisy_ set of default notification settings which
> sends an email to _every participant_ in an issue/discussion every time
> someone adds a comment. Instead, use the handy upvote button for discussions,
> and/or emoji reactions on both discussions and issues, which are a visible
> yet non-disruptive way to show your support.

If your issue hasn't been reported already, open an ["Issue Triage"] discussion
and make sure to fill in the template **completely**. They are vital for
maintainers to figure out important details about your setup.

> [!WARNING]
>
> A _very_ common mistake is to file a bug report either as a Q&A or a Feature
> Request. **Please don't do this.** Otherwise, maintainers would have to ask
> for your system information again manually, and sometimes they will even ask
> you to create a new discussion because of how few detailed information is
> required for other discussion types compared to Issue Triage.
>
> Because of this, please make sure that you _only_ use the "Issue Triage"
> category for reporting bugs — thank you!

[closed issues]: https://github.com/dfuentes87/freebsd-jails-tui/issues?q=is%3Aissue%20state%3Aclosed
[discussions]: https://github.com/dfuentes87/freebsd-jails-tui/discussions?discussions_q=is%3Aclosed
["Issue Triage"]: https://github.com/dfuentes87/freebsd-jails-tui/discussions/new?category=issue-triage

### I have an idea for a feature

Like bug reports, first search through both issues and discussions and try to
find if your feature has already been requested. Otherwise, open a discussion
in the ["Feature Requests, Ideas"] category.

["Feature Requests, Ideas"]: https://github.com/dfuentes87/freebsd-jails-tui/discussions/new?category=feature-requests-ideas

### I've implemented a feature

1. If there is an issue for the feature, open a pull request straight away.
2. If there is no issue, open a discussion and link to your branch.

### I have a question which is neither a bug report nor a feature request

Open an [General discussion](https://github.com/dfuentes87/freebsd-jails-tui/discussions/new?category=general).

> [!NOTE]
> If your question is about a missing feature, please open a discussion under
> the ["Feature Requests"] category. If the application is behaving
> unexpectedly, use the ["Error / Bug Report"] category.
>
> The "General" category is strictly for other kinds of discussions and do not
> require detailed information unlike the two other categories, meaning that
> maintainers would have to spend the extra effort to ask for basic information
> if you submit a bug report under this category.
>
> Therefore, please **pay attention to the category** before opening
> discussions to save us all some time and energy. Thank you!

## General Patterns

### Issues are for Maintainers

Unlike some other projects, this project **does not use the issue tracker for
discussion or feature requests**. Instead, we use GitHub
[discussions](https://github.com/dfuentes87/freebsd-jails-tui/discussions) for that.
Once a discussion reaches a point where a well-understood, actionable
item is identified, it is moved to the issue tracker. **This pattern
makes it easier for maintainers or contributors to find issues to work on
since _every issue_ is ready to be worked on.**

### Pull Requests Implement an Issue

Pull requests should be associated with a previously accepted issue.
**If you open a pull request for something that wasn't previously discussed,**
it may be closed or remain stale for an indefinite period of time. I'm not
saying it will never be accepted, but the odds are stacked against you.

Issues tagged with "feature" represent accepted, well-scoped feature requests.

> [!NOTE]
>
> **Pull requests are NOT a place to discuss feature design.** Please do
> not open a WIP pull request to discuss a feature. Instead, use a discussion
> and link to your branch.
