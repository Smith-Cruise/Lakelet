#!/bin/sh
# Lakelet installer.
#
# Downloads the prebuilt `lakelet` binary from GitHub Releases into the
# current directory:
#
#   curl -fsSL https://lakelet.dev/install.sh | sh
#
# Environment variables:
#   LAKELET_VERSION      Release tag to install (default: nightly)
#   LAKELET_INSTALL_DIR  Directory to place the binary in (default: .)
#
# Windows is not supported by this script; download the zip from
# https://github.com/Smith-Cruise/Lakelet/releases instead.

set -eu

REPO="Smith-Cruise/Lakelet"
VERSION="${LAKELET_VERSION:-nightly}"
INSTALL_DIR="${LAKELET_INSTALL_DIR:-.}"

info() {
    printf '\033[1;32m==>\033[0m %s\n' "$1" >&2
}

error() {
    printf '\033[1;31merror:\033[0m %s\n' "$1" >&2
    exit 1
}

os=$(uname -s)
arch=$(uname -m)

case "$arch" in
    arm64) arch="aarch64" ;;
    x86_64 | aarch64) ;;
    *) error "unsupported architecture: $arch (prebuilt binaries cover x86_64 and aarch64)" ;;
esac

case "$os" in
    Linux) target="${arch}-unknown-linux-gnu" ;;
    Darwin) target="${arch}-apple-darwin" ;;
    MINGW* | MSYS* | CYGWIN* | Windows_NT)
        error "this script does not support Windows; download lakelet-nightly-x86_64-pc-windows-msvc.zip from https://github.com/$REPO/releases"
        ;;
    *) error "unsupported operating system: $os" ;;
esac

# The x86_64 binaries are compiled for x86-64-v3 and need AVX2.
if [ "$arch" = "x86_64" ]; then
    case "$os" in
        Linux)
            grep -q avx2 /proc/cpuinfo 2>/dev/null ||
                error "the x86_64 build requires a CPU with AVX2 (x86-64-v3); build from source instead: https://lakelet.dev/getting-started/"
            ;;
        Darwin)
            if [ "$(sysctl -n hw.optional.avx2_0 2>/dev/null || echo 0)" != "1" ]; then
                error "the x86_64 build requires AVX2. If this Mac is Apple Silicon, run this script from a native (arm64) shell, not under Rosetta."
            fi
            ;;
    esac
fi

asset="lakelet-${VERSION}-${target}.tar.gz"
url="https://github.com/$REPO/releases/download/$VERSION/$asset"

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

info "Downloading $asset ($VERSION)"
if command -v curl >/dev/null 2>&1; then
    curl -fSL --progress-bar "$url" -o "$tmpdir/$asset"
elif command -v wget >/dev/null 2>&1; then
    wget -q "$url" -O "$tmpdir/$asset"
else
    error "neither curl nor wget is available"
fi

tar -xzf "$tmpdir/$asset" -C "$tmpdir" --strip-components=1

mkdir -p "$INSTALL_DIR"
dest="$INSTALL_DIR/lakelet"
if [ -e "$dest" ] && [ -t 0 ]; then
    printf '%s already exists. Overwrite? [y/N] ' "$dest" >&2
    read -r answer
    case "$answer" in
        y | Y | yes | YES) ;;
        *) error "aborted" ;;
    esac
fi

install -m 755 "$tmpdir/lakelet" "$dest"

info "Installed $("$dest" --version) to $dest"
info "Get started:"
printf '      %s\n' \
    "$dest --agent-help" \
    "https://lakelet.dev/getting-started/" >&2
printf '      %s\n' \
    "Move it onto your PATH for global use, e.g. mv $dest ~/.local/bin/" >&2
