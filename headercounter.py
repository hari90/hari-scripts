import pathlib
from collections import defaultdict
import re

file_headers = defaultdict(set)
headers_headers=defaultdict(set)
reverse_headers=defaultdict(set)
headers_use=defaultdict(int)
header_weight=defaultdict(int)
file_line_count=defaultdict(int)
weighted_header_use=defaultdict(int)

def count_headers():
    src_folder = pathlib.Path("src/yb")
    build_folder = pathlib.Path("build/latest/src/yb")
    all_file_paths = list(src_folder.rglob("*")) + list(build_folder.rglob("*"))
    all_file_paths = [ str(s) for s in all_file_paths ]
    
    file_paths = [ s for s in all_file_paths if (s.endswith(".h") or s.endswith(".cc")) and not s.endswith("_pch.h") and not s.endswith("_fwd.h") ]
    
    # Regular expression to match C++ #include statements
    include_pattern = re.compile(r'^\s*#include\s*"(.*?.h)"')
    
    for file_path in file_paths:
        count=0
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                normalizedfile_path=file_path.removeprefix("src/").removeprefix("build/latest/src/")
                for line in f:
                    count=count+1
                    match = include_pattern.match(line)
                    if match:
                        included_header = match.group(1)
                        file_headers[normalizedfile_path].add(included_header.removeprefix("src/"))
                file_line_count[normalizedfile_path]=count
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
    
    # Gets only headers
    for ffile in file_headers:
        if ffile.endswith(".h"):
            header_weight[ffile]=file_line_count[ffile]
            for header in file_headers[ffile]:
                headers_headers[ffile].add(header)
                reverse_headers[header].add(ffile)
                header_weight[ffile]+=file_line_count[header]
    
    for ffile in file_headers:
        for header in file_headers[ffile]:
            headers_use[header]=0
    
    def accumulate_counts(header, visited_headers):
        if (header not in visited_headers):
            visited_headers.add(header)
            headers_use[header]=headers_use[header]+1
            for other_header in headers_headers[header]:
                accumulate_counts(other_header, visited_headers)
    
    for ffile in file_headers:
        if not ffile.endswith(".h"):
            for header in file_headers[ffile]:
                accumulate_counts(header, set())
    
    # Compute weighted header usage
    for header in headers_use:
        weighted_header_use[header]=headers_use[header]*header_weight[header]

def print_dict_int_desc(dict_int):
    sorted_counts = sorted(dict_int.items(), key=lambda x: x[1], reverse=True)
    for key, count in sorted_counts:
        print(f"{key}: {count}")

def print_most_used_headers():
    for w in sorted(headers_use, key=headers_use.get, reverse=True):
        if not w.endswith("fwd.h"):
            if not w.startswith("yb/gutil") and not w.startswith("yb/util")  and not w.startswith("yb/common"):
                print(w, headers_use[w])

def print_most_used_headers_weighted():
    for w in sorted(weighted_header_use, key=weighted_header_use.get, reverse=True):
        if not w.endswith("fwd.h"):
            if not w.startswith("yb/gutil") and not w.startswith("yb/util")  and not w.startswith("yb/common")  and weighted_header_use[w] != 0:
                print(w, weighted_header_use[w])

def print_most_header_included():
    print_dict_int_desc(headers_headers)

def print_most_header_included_weighted():
    print_dict_int_desc(header_weight)

def analyze(header):
    print(f"{header}")
    print(f"Included in:      {headers_use[header]}")
    print(f"Weighted usage: {weighted_header_use[header]}")
    
    print(f"\nHeaders that it references:")
    weighted_children=defaultdict(int)
    for child in headers_headers[header]:
        weighted_children[child]=header_weight[child]
    print_dict_int_desc(weighted_children)

    print(f"\nHeaders that reference it:")
    weighted_parents=defaultdict(int)
    for parent in reverse_headers[header]:
        weighted_parents[parent]=header_weight[parent]
    print_dict_int_desc(weighted_parents)

analyze('yb/master/catalog_manager.h')

def main():
    count_headers()
    # print_most_used_headers_weighted()
    analyze('yb/master/catalog_manager.h')
    analyze('yb/master/catalog_manager_if.h')

if __name__ == "__main__":
    main()


