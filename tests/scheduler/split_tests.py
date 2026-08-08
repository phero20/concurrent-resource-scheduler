import os
import re

filepath = 'scheduler_test.go'

with open(filepath, 'r') as f:
    content = f.read()

# Define the imports block
imports = """package scheduler_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/feroz/concurrent-resource-scheduler/config"
	"github.com/feroz/concurrent-resource-scheduler/errors"
	"github.com/feroz/concurrent-resource-scheduler/placement"
	"github.com/feroz/concurrent-resource-scheduler/scheduler"
)
"""

# Extract the helpers
helpers_pattern = r"(type Resource struct \{.*?func validConfig\[\^\{]*?\{.*?\})"
# Actually, validConfig regex could be tricky. Let's just slice it explicitly.
# Looking at the file, helpers start at line 14 and end before "func TestNewScheduler"
end_idx = content.find("func TestNewScheduler")
helpers = content[content.find("type Resource"):end_idx].strip()

# Extract stringAffinity helper which is located around line 740
affinity_helper_pattern = r"(type stringAffinity string\n\nfunc \(s stringAffinity\) AppendAffinityBytes\(dst \[\]byte\) \[\]byte \{\n\treturn append\(dst, s\.\.\.\)\n\})"
m2 = re.search(affinity_helper_pattern, content)
if m2:
    helpers += "\n\n" + m2.group(1)

# Create helper_test.go
with open('helper_test.go', 'w') as f:
    f.write(imports + "\n" + helpers + "\n")

# Now extract all test functions
functions = []

lines = content.split('\n')
in_func = False
current_func = []
brace_count = 0
func_name = ""

for line in lines:
    if line.startswith('func Test'):
        in_func = True
        func_name = re.match(r'func (Test[a-zA-Z0-9_]+)', line).group(1)
        current_func = [line]
        brace_count = line.count('{') - line.count('}')
    elif in_func:
        current_func.append(line)
        brace_count += line.count('{') - line.count('}')
        if brace_count == 0:
            in_func = False
            functions.append((func_name, '\n'.join(current_func)))

def write_file(filename, test_names):
    global functions
    funcs = []
    for tn in test_names:
        for f in functions:
            if f[0].startswith(tn) and f not in funcs:
                funcs.append(f)
    if funcs:
        with open(filename, 'w') as f:
            f.write(imports + "\n" + "\n\n".join([f[1] for f in funcs]) + "\n")
        # Remove matched funcs so we know what's left
        functions = [f for f in functions if f not in funcs]

write_file('scheduler_constructor_test.go', ['TestNewScheduler'])
# Note: the user asked to put TestAcquireByAffinity under scheduler_acquire_test.go
write_file('scheduler_acquire_test.go', ['TestAcquire_', 'TestAcquireByAffinity'])
write_file('scheduler_add_test.go', ['TestAdd_', 'TestBatchAdd_'])
write_file('scheduler_release_test.go', ['TestRelease_'])
write_file('scheduler_mutation_test.go', ['TestUpdate_', 'TestRemove_', 'TestExclude', 'TestInclude'])
write_file('scheduler_query_test.go', ['TestGet', 'TestLen', 'TestStats'])
write_file('scheduler_shutdown_test.go', ['TestShutdown'])
# Grab stress tests
write_file('scheduler_stress_test.go', ['TestScheduler_MixedConcurrentStress', 'TestBatchAdd_ConcurrentStress'])

# If any test functions remain, put them in a leftover file to not lose them
if functions:
    with open('scheduler_leftover_test.go', 'w') as f:
        f.write(imports + "\n" + "\n\n".join([f[1] for f in functions]) + "\n")

# If everything worked, rename the original file so it's not compiled
os.rename('scheduler_test.go', 'scheduler_test.go.bak')
