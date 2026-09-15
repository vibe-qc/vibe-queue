# Site configuration

Keep real hostnames, users, scheduler accounts, deployment keys and filesystem locations outside the code checkout. The public example contains only generic values.

vibe-queue already reads an external config.toml. The default directory is ~/.config/vq (or XDG_CONFIG_HOME/vq). Set VQ_CONFIG_DIR to another directory when needed. Multi-user installations use /etc/vq/config.toml. The repository does not need the real file to be built or tested.

```sh
mkdir -p "$HOME/.config/vq"
cp config/public-example.toml "$HOME/.config/vq/config.toml"
# Edit the external copy for your site.
```

Store real site files in your private configuration management system. Do not commit them to this public source repository. See CONTRIBUTING.md for contribution requirements.
