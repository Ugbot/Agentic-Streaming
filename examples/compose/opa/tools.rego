# Agentic Streaming tool policy, for the OPA lab. The workflow document's `tools` is the floor: this
# can only narrow it. Anything but `allow` true refuses the call, and so does an OPA that is down.
package agentic.tools

import rego.v1

default allow := false

# Reading is fine for anyone with a principal.
allow if {
	input.effect == "read"
	input.principal != null
}

# A payment is for the payments role, and never over the limit.
allow if {
	input.tool == "pay"
	"payments" in input.principal.roles
	input.args.amount <= 100
}

reason := sprintf("tool %s refused for %s", [input.tool, object.get(input.principal, "subject", "nobody")])
