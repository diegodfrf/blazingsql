#!/bin/bash
# CheckStyle for new and modified files
# ./checkstyle [options]

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
function CheckStyleBlack {
    BLACK=$(black --check "$1" --diff --color)
}

function ApplyStyleBlack {
    BLACK=$(black "$1")
}

function CheckStyleFlake8 {
    FLAKE8=$(flake8 --config=pyblazing/.flake8 --check "$1")
}

function CheckStyleClangFormat {
    clang-format "$1" > "$2"
    CLANG_FORMAT_DIFF=$(diff "$1" "$2")
}

function ApplyStyleClangFormat {
    clang-format -i "$1" -fallback-style=none
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
                CheckStyleBlack "$file"
                echo -e ">>>>>>>>BLACK BEGIN DIFF [$file]>>>>>>>"
                echo -e "${BLACK}"
                echo -e ">>>>>>>>BLACK END DIFF [$file]>>>>>>>"
                CheckStyleFlake8 "$file"
                echo -e ">>>>>>>>FLAKE8 BEGIN DIFF [$file]>>>>>>>"
                echo -e "${FLAKE8}"
                echo -e ">>>>>>>>FLAKE8 END DIFF [$file]>>>>>>>"
            elif [[ $extension =~ (cu|cuh|h|hpp|cpp|inl) ]]; then
                CheckStyleClangFormat "$file" "$tmpDir/$filename"
                echo -e ">>>>>>>>CLANG-FORMAT BEGIN DIFF [$file]>>>>>>>"
                echo -e "${CLANG_FORMAT_DIFF}"
                echo -e "<<<<<<<<CLANG-FORMAT END DIFF [$file]<<<<<<<"
            fi
        fi
    done
    echo -e "Cleanup temporal files"
    rm -rf "$tmpDir"
    echo -e "You have 2 options to fix checkstyle observations"
    echo -e "1. Apply checkstyle executing ./scripts/checkstyle.sh [file] --fix a the commit your changes"
    echo -e "2. Manually update the file to conform the checkstyle rules"
fi
if hasArg --fix; then
    for file in "${files[@]}";
    do
        if test -f "$file"; then
            #check extension
            filename=$(basename -- "$file")
            extension="${filename##*.}"
            if [ "$extension" == "py" ]; then
                echo -e "Applying black style to $file"
                ApplyStyleBlack "$file"
            elif [[ $extension =~ (cu|cuh|h|hpp|cpp|inl) ]]; then
                ApplyStyleClangFormat "$file"
                echo -e "Applying clang-format to $file"
            else
                echo -e "File $file skipped!!!"
            fi
        fi
    done
    echo -e "Apply style complete!!!"
fi
