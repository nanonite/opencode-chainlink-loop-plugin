# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added
- Add local checkout loading for OpenCode 2 (#1)
- Add the TUI sidebar/palette for the loop plugin (cli.json) (#2)

### Fixed
- Fix chainlink-loop dying with `is in phase worker` when a reviewer step returns no session ID, and log a failed step's exit code, stderr and duration (#6)
- Fix the chainlink-loop CLI silently dropping inner-loop progress lines (#5)
- Fix extractReview discarding genuine reviewer approvals when prose with brackets precedes the JSON verdict (#4)

### Changed
- Publish the plugin under a controlled npm scope and recheck upstream install fixes (#3)
