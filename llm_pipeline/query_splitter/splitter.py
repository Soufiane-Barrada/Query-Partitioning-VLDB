
# import json
# import re
# from langchain_core.messages import SystemMessage, HumanMessage
# from .rits_chat import RITSChat
# from .prompts import system_base, system_schema, system_tables_info, system_output

# class QuerySplitter:
#     def __init__(self, endpoint_url: str, model_name: str):
#         self.llm = RITSChat(endpoint_url=endpoint_url, model_name=model_name)

#     def split_query(self, sql_query: str) -> dict[str, str]:
#         """Return {"sql1": "...", "sql2": "..."}"""
#         result = self.llm.invoke([
#             SystemMessage(content=system_base + "\n" + system_schema + system_tables_info + system_output),
#             HumanMessage(content=sql_query)
#         ])
#         raw = result.content.strip()

#         # Strip markdown fences if present
#         if raw.startswith("```"):
#             raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
#             raw = re.sub(r"```$", "", raw).strip()

#         try:
#             return json.loads(raw)
#         except Exception as e:
#             raise ValueError(f"LLM did not return valid JSON.\nRaw output:\n{result.content}") from e


import json
import re
from typing import Any, Dict
from langchain_core.messages import SystemMessage, HumanMessage
from .rits_chat import RITSChat
from .prompts import system_base, system_output


class QuerySplitter:
    def __init__(self,
                 endpoint_url: str,
                 model_name: str,
                 system_schema_override: str,
                 system_tables_info_override: str):
        self.llm = RITSChat(endpoint_url=endpoint_url, model_name=model_name)
        self.system_schema = system_schema_override
        self.system_tables_info = system_tables_info_override

    def split_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Return either:
          {"has_cut": False}
        or
          {"has_cut": True, "sql1": "...", "sql2": "...", "q1_engine": "...", "q2_engine": "..."}
        """
        result = self.llm.invoke([
            SystemMessage(content=system_base + "\n" + self.system_schema + self.system_tables_info + system_output),
            HumanMessage(content=sql_query)
        ])
        raw = result.content.strip()

        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
            raw = re.sub(r"```$", "", raw).strip()

        try:
            obj = json.loads(raw)
        except Exception as e:
            raise ValueError(f"LLM did not return valid JSON.\nRaw output:\n{result.content}") from e

        if not isinstance(obj, dict):
            raise ValueError(f"LLM output must be a JSON object, got: {type(obj)}")

        if "has_cut" not in obj:
            raise ValueError(f"LLM output missing required key 'has_cut'. Raw:\n{result.content}")

        has_cut = bool(obj["has_cut"])
        if not has_cut:
            return {"has_cut": False}

        # has_cut == True
        required = ("sql1", "sql2", "q1_engine", "q2_engine")
        missing = [k for k in required if k not in obj]
        if missing:
            raise ValueError(f"LLM output missing keys {missing}. Raw:\n{result.content}")

        sql1 = obj["sql1"]
        sql2 = obj["sql2"]
        q1 = str(obj["q1_engine"]).strip().lower()
        q2 = str(obj["q2_engine"]).strip().lower()

        if not isinstance(sql1, str) or not isinstance(sql2, str):
            raise ValueError(f"sql1/sql2 must be strings. Raw:\n{result.content}")

        if q1 not in ("duckdb", "datafusion") or q2 not in ("duckdb", "datafusion"):
            raise ValueError(f"q1_engine/q2_engine must be duckdb|datafusion. Raw:\n{result.content}")

        return {
            "has_cut": True,
            "sql1": sql1,
            "sql2": sql2,
            "q1_engine": q1,
            "q2_engine": q2,
        }
