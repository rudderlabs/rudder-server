#!/usr/bin/env bash

# ObjectBox libraries are available as GitHub release artifacts.
# This script downloads the current version of the library and extracts/installs it locally.
# The download happens in a "download" directory.
# After download and extraction, the script asks if the lib should be installed in /usr/local/lib.
#
# Windows note: to run this script you need to install a bash like "Git Bash".
# Plain MINGW64, Cygwin, etc. might work too, but was not tested.

set -eu

#default values
quiet=false
printHelp=false
libBuildDir="$(pwd)/lib"
variant=
###

while :; do
    case ${1:-} in
        -h|--help)
          printHelp=true
        ;;
        --quiet)
          quiet=true
        ;;
        --install)
          quiet=true
          installLibrary=true
        ;;
        --uninstall)
          uninstallLibrary=true
        ;;
        --sync)
          variant="sync-"
        ;;
        *) break
    esac
    shift
done

tty -s || quiet=true

# Note: optional arguments like "--quiet" shifts argument positions in the case block above

version=${1:-0.16.0}
os=${2:-$(uname)}
arch=${3:-$(uname -m)}
echo "Base config: OS ${os} and architecture ${arch}"

if [[ "$os" == MINGW* ]] || [[ "$os" == CYGWIN* ]]; then
    os=Windows
    echo "Adjusted OS to $os"
elif [[ "$os" == "Darwin" ]]; then
    os=MacOS
    arch=universal
    echo "Adjusted OS to $os and architecture to $arch"
fi

if [[ $arch == "x86_64" ]]; then
    arch=x64
elif [[ $arch == armv7* ]]; then
    arch=armv7hf
    echo "Selected ${arch} architecture for download (hard FP only!)"
elif [[ $arch == armv6* ]]; then
    arch=armv6hf
    echo "Selected ${arch} architecture for download (hard FP only!)"
fi

# sudo might not be defined (e.g. when building a docker image)
sudo="sudo"
if [ ! -x "$(command -v sudo)" ]; then
    sudo=""
fi

# original location where we installed in previous versions of this script
oldLibDir=

if [[ "$os" = "MacOS" ]]; then
    libFileName=libobjectbox.dylib
    libDirectory=/usr/local/lib
elif [[ "$os" = "Windows" ]]; then
    libFileName=objectbox.dll

    # this doesn't work in Git Bash, fails silently
    # sudo="runas.exe /user:administrator"
    # libDirectory=/c/Windows/System32

    # try to determine library path based on gcc.exe path
    libDirectory=""
    if [ -x "$(command -v gcc)" ] && [ -x "$(command -v dirname)" ] && [ -x "$(command -v realpath)" ]; then
        libDirectory=$(realpath "$(dirname "$(command -v gcc)")/../lib")
    fi
else
    libFileName=libobjectbox.so
    libDirectory=/usr/lib
    oldLibDir=/usr/local/lib
fi


function printUsage() {
    echo "download.sh [\$1:version] [\$2:os] [\$3:arch]"
    echo
    echo "  Options (use at front only):"
    echo "    --sync: download ObjectBox Sync variant of the library"
    echo "    --quiet: skipping asking to install to ${libDirectory}"
    echo "    --install: install library to ${libDirectory}"
    echo "    --uninstall: uninstall from ${libDirectory}"
}

if ${printHelp} ; then
    printUsage
    exit 0
fi

function uninstallLib() {
    dir=${1}

    if ! [ -f "${dir}/${libFileName}" ] ; then
        echo "${dir}/${libFileName} doesn't exist, can't uninstall"
        exit 1
    fi

    echo "Removing ${dir}/${libFileName}"

    if [ -x "$(command -v ldconfig)" ]; then
        linkerUpdateCmd="ldconfig ${dir}"
    else
        linkerUpdateCmd=""
    fi

    $sudo bash -c "rm -fv '${dir}/${libFileName}' ; ${linkerUpdateCmd}"
}

if ${uninstallLibrary:-false}; then
    uninstallLib "${libDirectory}"

    if [ -x "$(command -v ldconfig)" ]; then
        libInfo=$(ldconfig -p | grep "${libFileName}" || true)
    else
        libInfo=$(ls -lh ${libDirectory}/* | grep "${libFileName}" || true) # globbing would give "no such file" error
    fi

    if [ -z "${libInfo}" ]; then
        echo "Uninstall successful"
    else
        echo "Uninstall failed, leftover files:"
        echo "${libInfo}"
        exit 1
    fi

    exit 0
fi

downloadDir=download

while getopts v:d: opt
do
    case $opt in
        d) downloadDir=$OPTARG;;
        v) version=$OPTARG;;
        *) printUsage
           exit 1 ;;
   esac
done

conf=$(echo "${os}-${arch}" | tr '[:upper:]' '[:lower:]')   # convert to lowercase

SUPPORTED_PLATFORMS="
linux-x64
linux-armv6hf
linux-armv7hf
linux-aarch64
windows-x86
windows-x64
macos-universal
" #SUPPORTED_PLATFORMS
if [ -z "$( awk -v key="${conf}" '$1 == key {print $NF}' <<< "$SUPPORTED_PLATFORMS" )" ]; then
    echo "Warning: platform configuration ${conf} is not listed as supported."
    echo "Trying to continue with the download anyway, maybe the list is out of date."
    echo "If that doesn't work you can select the configuration manually (use --help for details)"
    echo "Possible values are:"
    awk '$0 {print " - " $1 }'  <<< "$SUPPORTED_PLATFORMS"
    exit 1
fi

conf="${variant}${conf}"
echo "Using configuration ${conf}"

if [[ "$os" == "Linux"  ]]; then
  archiveExt=tar.gz
else
  archiveExt=zip
fi

targetDir="${downloadDir}/objectbox-${version}-${conf}"
archiveFile="${targetDir}.${archiveExt}"
downloadUrl="https://github.com/objectbox/objectbox-c/releases/download/v${version}/objectbox-${conf}.${archiveExt}"
echo "Resolved URL: ${downloadUrl}"
echo "Downloading ObjectBox library version v${version} for ${conf}..."
mkdir -p "$(dirname "${archiveFile}")"

# Support both curl and wget because their availability is platform dependent
if [ -x "$(command -v curl)" ]; then
    curl --location --fail --output "${archiveFile}" "${downloadUrl}"
else
    wget --no-verbose --output-document="${archiveFile}" "${downloadUrl}"
fi

if [[ ! -s ${archiveFile} ]]; then
    echo "Error: download failed (file ${archiveFile} does not exist or is empty)"
    exit 1
fi

echo "Downloaded:"
du -h "${archiveFile}"

echo
echo "Extracting into ${targetDir}..."
mkdir -p "${targetDir}"

if [[ "$archiveExt" == "zip" ]]; then
  unzip "${archiveFile}" -d "${targetDir}"
else
  tar -xzf "${archiveFile}" -C "${targetDir}"
fi

if [ ! -d "${libBuildDir}"  ] || ${quiet} ; then
    mkdir -p "${libBuildDir}"
    cp "${targetDir}"/lib/* "${libBuildDir}"
    echo "Copied to ${libBuildDir}:"
    ls -l "${libBuildDir}"
else
    read -p "${libBuildDir} already exists. Copy the just downloaded library to it? [Y/n] " -r
    if [[ $REPLY =~ ^[Yy]$ ]] || [[ -z "$REPLY" ]] ; then
        cp "${targetDir}"/lib/* "${libBuildDir}"
        echo "Copied; contents of ${libBuildDir}:"
        ls -l "${libBuildDir}"
    fi
fi

if ${quiet} ; then
    if ! ${installLibrary:-false}; then
        echo "Skipping installation to ${libDirectory} in quiet mode"
        if [ -f "${libDirectory}/${libFileName}" ]; then
            echo "However, you have a library there:"
            ls -l "${libDirectory}/${libFileName}"
        fi
    fi
else
    if [ -z "${installLibrary:-}" ] && [ -n "${libDirectory}" ]; then
        read -p "OK. Do you want to install the library into ${libDirectory}? [Y/n] " -r
        if [[ $REPLY =~ ^[Yy]$ ]] || [[ -z "$REPLY" ]] ; then
            installLibrary=true

            if [ -n "${oldLibDir}" ] && [ -f "${oldLibDir}/${libFileName}" ] ; then
                echo "Found an old installation in ${oldLibDir} but a new one is going to be placed in ${libDirectory}."
                echo "It's recommended to uninstall the old library to avoid problems."
                read -p "Uninstall from old location? [Y/n] " -r
                if [[ $REPLY =~ ^[Yy]$ ]] || [[ -z "$REPLY" ]] ; then
                    uninstallLib ${oldLibDir}
                fi
            fi

        fi
    fi
fi

if ${installLibrary:-false}; then
    echo "Installing ${libDirectory}/${libFileName}"
    $sudo cp "${targetDir}/lib/${libFileName}" ${libDirectory}

    if [ -x "$(command -v ldconfig)" ]; then
        $sudo ldconfig "${libDirectory}"
        libInfo=$(ldconfig -p | grep "${libFileName}" || true)
    else
        libInfo=$(ls -lh ${libDirectory}/* | grep "${libFileName}" || true) # globbing would give "no such file" error
    fi

    if [ -z "${libInfo}" ]; then
        echo "Error installing the library - not found"
        exit 1
    else
        echo "Installed objectbox libraries:"
        echo "${libInfo}"
    fi
fi