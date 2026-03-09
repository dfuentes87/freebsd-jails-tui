# freebsd-jails-tui

Terminal UI for creating and managing FreeBSD jails, built in Go.

This initial milestone implements the **main dashboard**:

- Scrollable jail list (`j/k`, arrows, page up/down)
- Live metrics refresh every 2 seconds
- Colorized status badges (`[+]` running, `[-]` stopped)
- JID-based running detection (a jail with a JID is treated as running)
- Quick detail panel for the selected jail
- Dedicated jail detail view that consolidates `jls`, `jail.conf`, `zfs`, and `rctl`
- Jail creation wizard with 6 guided steps
- Save/load wizard templates for repeated jail setups
- ZFS integration panel for snapshot and rollback actions
- Help/shortcuts screen (`h` or `?`)

## Requirements

- FreeBSD host with jails configured/running
- Go 1.25+

## Run

```bash
go mod tidy
go run .
```

## Data sources

- `/etc/jail.conf` and `/usr/local/etc/jail.conf` for configured jail names
- `/etc/jail.conf.d/*` and `/usr/local/etc/jail.conf.d/*` for additional jail definitions
- `jls -n` for running jail/JID metadata
- `ps -axo jid=,pcpu=,rss=` for per-jail CPU and memory metrics
- `zfs list` for mapped jail dataset usage/quota details in detail view
- `rctl` for jail-specific resource control rules in detail view

## Keybindings

- `j` / `k` or `up` / `down`: move selection
- `pgdown` / `pgup`: scroll page
- `g` / `G`: first/last jail
- `enter` / `d`: open full jail detail view
- `c`: open jail creation wizard
- `?`: open help/shortcuts page from any screen
- `h`: open help/shortcuts page from non-edit screens
- `r`: immediate refresh
- `q`: quit

### Jail detail view

- `j` / `k` or `up` / `down`: scroll details
- `pgdown` / `pgup`: scroll a page
- `g` / `G`: top/bottom
- `r`: refresh detail data
- `z`: open ZFS integration panel
- `esc` / `backspace`: return to dashboard

### ZFS integration panel

- `j` / `k` or `up` / `down`: select snapshot
- `n`: create snapshot (prompts for snapshot name)
- `r`: begin rollback to selected snapshot (with confirmation)
- `enter`: confirm create/rollback action
- `R`: refresh snapshot list
- `esc`: cancel prompt/confirmation or return to detail view

### Jail creation wizard

- `tab` / `shift+tab`: move active field
- `enter` / `right`: next step
- `left`: previous step
- `s` (step 6/confirmation): save current wizard values as a template
- `l` (step 6/confirmation): load a saved template
- `backspace`: delete character in active field
- `esc`: cancel wizard and return to dashboard
- On step 1, `Dataset` expects a full ZFS dataset path (example: `zroot/jails/web01`)

### Templates

- Templates are persisted in `templates.json` under your user config directory:
  - `$XDG_CONFIG_HOME/freebsd-jails-tui/templates.json` when `XDG_CONFIG_HOME` is set
  - otherwise `~/.config/freebsd-jails-tui/templates.json`
- Loading a template populates all wizard steps (name, dataset, release/template, networking, limits, mounts)

### Help/shortcuts page

- `?`: open from any screen
- `h`: open from non-edit screens (dashboard/detail/ZFS list)
- `j` / `k` or `pgup` / `pgdown`: scroll
- `esc` or `enter`: close help and return to previous screen

### Wizard execution behavior

- On step 6, `enter` executes jail creation commands (destructive operations)
- The wizard now creates/uses ZFS dataset paths, writes `/etc/jail.conf.d/<name>.conf`, optionally writes `/etc/fstab.<name>`, starts the jail, and applies `rctl` limits
- Run the TUI as root (or with equivalent privileges) for these operations

## Next milestones

- Create jail workflow
- Start/stop/restart actions
- Destroy and edit jail configuration
- Confirmation dialogs and command logs
