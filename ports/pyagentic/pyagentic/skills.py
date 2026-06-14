"""Skills — a named bundle of (tools + a system-prompt fragment + required facts), the
portable analogue of the Flink ``Skill``. A path can declare ``skills: [billing]`` and
the builder expands them into the path's tool set + prompt, so capabilities compose
without rewiring each agent.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Sequence, Tuple


@dataclass(frozen=True)
class Skill:
    name: str
    tools: Tuple[str, ...] = ()
    prompt_fragment: str = ""
    required_facts: Tuple[str, ...] = ()


class SkillRegistry:
    """Named skills, looked up by the builder when a path lists ``skills``."""

    def __init__(self) -> None:
        self._skills: Dict[str, Skill] = {}

    def register(self, skill: Skill) -> "SkillRegistry":
        self._skills[skill.name] = skill
        return self

    def get(self, name: str) -> Skill:
        if name not in self._skills:
            raise KeyError(f"unknown skill {name!r}")
        return self._skills[name]

    def expand(self, names: Sequence[str]) -> Tuple[List[str], str, List[str]]:
        """Resolve skill names → (extra tool ids, joined prompt fragment, required facts)."""
        tools: List[str] = []
        fragments: List[str] = []
        facts: List[str] = []
        for n in names:
            s = self.get(n)
            for t in s.tools:
                if t not in tools:
                    tools.append(t)
            if s.prompt_fragment:
                fragments.append(s.prompt_fragment)
            facts.extend(s.required_facts)
        return tools, "\n".join(fragments), facts

    @classmethod
    def from_specs(cls, specs: List[dict]) -> "SkillRegistry":
        """Build from the YAML ``skills:`` list ([{name, tools, prompt, facts}])."""
        reg = cls()
        for s in specs or []:
            reg.register(Skill(
                name=s["name"],
                tools=tuple(s.get("tools", [])),
                prompt_fragment=s.get("prompt", ""),
                required_facts=tuple(s.get("facts", [])),
            ))
        return reg
