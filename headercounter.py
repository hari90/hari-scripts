import pathlib
from collections import defaultdict
import re

file_headers = defaultdict(list)
headers_headers=defaultdict(list)
headers_use=defaultdict(list)
header_weight=defaultdict(int)
file_line_count=defaultdict(int)
weighted_header_use=defaultdict(int)

def count_headers():
    src_folder = pathlib.Path("src/yb")
    all_file_paths = list(src_folder.rglob("*"))
    all_file_paths = [ str(s) for s in all_file_paths ]
    
    pat = re.compile(r'^(?!.*(_fwd\.h|_pch\.h)$).*\.(h|cc)$')
    
    file_paths = [ s for s in all_file_paths if (s.endswith(".h") or s.endswith(".cc")) and not s.endswith("_pch.h") and not s.endswith("_fwd.h") ]
    
    # Regular expression to match C++ #include statements
    include_pattern = re.compile(r'^\s*#include\s*"(.*?.h)"')
    
    for file_path in file_paths:
        count=0
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                normalizedfile_path=file_path.removeprefix("src/")
                for line in f:
                    count=count+1
                    match = include_pattern.match(line)
                    if match:
                        included_header = match.group(1)
                        file_headers[normalizedfile_path].append(included_header.removeprefix("src/"))
                file_line_count[normalizedfile_path]=count
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
    
    # Gets only headers
    for ffile in file_headers:
        if ffile.endswith(".h"):
            header_weight[ffile]=file_line_count[ffile]
            for header in file_headers[ffile]:
                headers_headers[ffile].append(header)
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
    weighted_header_use=defaultdict(int)
    for header in headers_use:
        weighted_header_use[header]=headers_use[header]*header_weight[header]
    
    # Print most used headers
    for w in sorted(headers_use, key=headers_use.get, reverse=True):
        if not w.endswith("fwd.h"):
            if not w.startswith("yb/gutil") and not w.startswith("yb/util")  and not w.startswith("yb/common"):
                print(w, headers_use[w])


count_headers()


headers_use['yb/master/catalog_manager.h']


# Print most used headers
for w in sorted(headers_use, key=headers_use.get, reverse=True):
    if not w.endswith("fwd.h"):
        if not w.startswith("yb/gutil") and not w.startswith("yb/util")  and not w.startswith("yb/common"):
            print(w, headers_use[w])

# Print most used headers weighted
for w in sorted(weighted_header_use, key=weighted_header_use.get, reverse=True):
    if not w.endswith("fwd.h"):
        if not w.startswith("yb/gutil") and not w.startswith("yb/util")  and not w.startswith("yb/common"):
            print(w, weighted_header_use[w])


# Count how many headers each header includes
header_includes_count = {header: len(headers) for header, headers in headers_headers.items()}
sorted_headers = sorted(header_includes_count.items(), key=lambda x: x[1], reverse=True)
for header, count in sorted_headers:
    print(f"{header}: {count}")


# Count how many headers each header includes based on num lines included
header_includes_count = {header: headers for header, headers in header_weight.items()}
sorted_headers = sorted(header_includes_count.items(), key=lambda x: x[1], reverse=True)
for header, count in sorted_headers:
    print(f"{header}: {count}")


# Store the list to a file
with open("file_headers.txt", "w", encoding="utf-8") as f:
    for file, headers in file_headers.items():
        f.write(f"{file}:\n")
        for header in headers:
            f.write(f"  {header}\n")
        f.write("\n")  # Add a blank line for readability


# Read back from the file
with open("file_headers.txt", "r", encoding="utf-8") as f:
    current_file = None
    for line in f:
        line = line.strip()
        if line.endswith(":"):  # Detect file name
            current_file = line[:-1]  # Remove trailing colon
            file_headers[current_file] = []
        elif current_file and line:  # Add headers to the current file
            file_headers[current_file].append(line)


