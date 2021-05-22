#!/bin/bash
# CheckStyle for new and modified files
# ./checkstyle [options] [file]

NUMARGS=$#
ARGS=$*

function hasArg {
    (( ${NUMARGS} != 0 )) && (echo " ${ARGS} " | grep -q " $1 ")
}

function dependencies() {
    if ! which flake8; then
        echo -e "Installing flake8"
        pip install flake8
    elif ! which black; then
        echo -e "Installing black"
        pip install black
    elif ! which clang-format; then
        echo -e "Installing clang-format-8"
        pip install clang-format==8.0.1
    fi
}

function getFiles() {
    if ! which git;then
        echo -e "git is not found please install it!"
    else
        # List stagged files for commit
        mapfile -t files < <(git diff --name-only --cached)
        return "$files"
    fi
}
function StyleBlack {
    BLACK=$(black --check "$1" --diff)
    BLACK_RETVAL=$?
}

function StyleFlake8 {
    FLAKE8=$(black --check "$1" --diff)
    FLAKE8_RETVAL=$?
}

function StyleClangFormat {
    CLANG_FORMAT=$(clang-format "$1" > "$2")
    CLANG_FORMAT_RETVAL=$?
    CLANG_FORMAT_DIFF=$(diff "$1" "$2")
}

# Install dependencies
dependencies
declare -a files=getFiles
if hasArg --check; then
    tmpDir=$(mktemp -d)
    for file in "${files[@]}";
    do
        if test -f "$file"; then
            #check extension
            filename=$(basename -- "$file")
            extension="${filename##*.}"
            if [ "$extension" == "py" ]; then
                StyleBlack "$file"
                echo -e "Black check Style >>>> ${BLACK}"
                StyleFlake8 "$file"
                echo -e "Flake8 check Style >>>> ${FLAKE8}"
            elif [[ $extension =~ (cu|cuh|h|hpp|cpp|inl) ]]; then
                StyleClangFormat "$file" "$tmpDir/$filename"
                echo -e "DIFF >> ${CLANG_FORMAT_DIFF}"
            fi
        fi
    done
fi
