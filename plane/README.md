<a href="https://plane.dev">
    <img src="../resources/plane-logo-light.svg" alt="Plane logo" />
</a>

[![GitHub Repo stars](https://img.shields.io/github/stars/drifting-in-space/plane?style=social)](https://github.com/drifting-in-space/plane)
[![Docker image](https://img.shields.io/docker/v/plane/plane)](https://hub.docker.com/r/plane/plane/tags)
[![Build Docker Image](https://github.com/drifting-in-space/plane/actions/workflows/build-image.yml/badge.svg)](https://github.com/drifting-in-space/plane/actions/workflows/build-image.yml)
[![Tests](https://github.com/drifting-in-space/plane/actions/workflows/tests.yml/badge.svg)](https://github.com/drifting-in-space/plane/actions/workflows/tests.yml)
[![Chat on Discord](https://img.shields.io/static/v1?label=chat&message=discord&color=404eed)](https://discord.gg/N5sEpsuhh9)

[Plane](https://plane.dev) is a distributed system for **running stateful WebSocket backends at scale**. Plane is heavily inspired by [Figma’s mulitplayer infrastructure](https://www.figma.com/blog/rust-in-production-at-figma/), which dynamically spawns a process for each active document.

Use cases include:
- Scaling up [authoritative multiplayer backends](https://driftingin.space/posts/you-might-not-need-a-crdt).
- Running isolated code environments (like REPLs, code notebooks, and LLM agent sandboxes).
- Data-intensive applications that need a dedicated high-RAM process for each active user session.

Read more about [Plane’s architecture](https://plane.dev/concepts/architecture).

[![Architecture diagram of Plane](../docs/public/arch-diagram.svg)](https://plane.dev/concepts/architecture)