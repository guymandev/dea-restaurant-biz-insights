import json
import re
from pathlib import Path


INPUT_PATH = Path("docs/glue_workflow_restaurant_daily_pipeline.json")
OUTPUT_PATH = Path("docs/workflow_orchestration.md")


def safe_node_id(name: str) -> str:
    """
    Mermaid node IDs must be simple-ish.
    Keep labels readable separately from IDs.
    """
    return re.sub(r"[^A-Za-z0-9_]", "_", name)


def mermaid_label(name: str) -> str:
    """
    Wrap labels safely for Mermaid.
    """
    return name.replace('"', '\\"')


def get_workflow(payload: dict) -> dict:
    """
    AWS get-workflow returns:
      {
        "Workflow": {
          ...
          "Graph": {
             "Nodes": [...],
             "Edges": [...]
          }
        }
      }
    """
    return payload.get("Workflow", payload)


def get_graph(workflow: dict) -> dict:
    return workflow.get("Graph", {})


def extract_nodes(graph: dict) -> dict:
    """
    Returns a map of UniqueId -> node.
    """
    nodes = graph.get("Nodes", [])
    return {
        node.get("UniqueId", node.get("Name")): node
        for node in nodes
    }


def extract_mermaid_from_edges(graph: dict) -> list[str]:
    """
    Build Mermaid lines from Glue graph edges.
    This gives us the actual workflow structure as AWS sees it.
    """
    nodes_by_id = extract_nodes(graph)
    edges = graph.get("Edges", [])

    lines = [
        "```mermaid",
        "flowchart LR",
    ]

    # Define nodes first.
    for node_id, node in nodes_by_id.items():
        name = node.get("Name", node_id)
        node_type = node.get("Type", "UNKNOWN")
        mermaid_id = safe_node_id(node_id)
        label = mermaid_label(name)

        if node_type == "TRIGGER":
            lines.append(f'    {mermaid_id}{{"{label}"}}')
        elif node_type == "JOB":
            lines.append(f'    {mermaid_id}["{label}"]')
        elif node_type == "CRAWLER":
            lines.append(f'    {mermaid_id}[/"{label}"/]')
        else:
            lines.append(f'    {mermaid_id}(("{label}"))')

    lines.append("")

    # Add edges.
    for edge in edges:
        source_id = edge.get("SourceId")
        dest_id = edge.get("DestinationId")

        if not source_id or not dest_id:
            continue

        source = safe_node_id(source_id)
        dest = safe_node_id(dest_id)

        lines.append(f"    {source} --> {dest}")

    lines.append("```")

    return lines


def trigger_logic(trigger: dict) -> str:
    predicate = trigger.get("Predicate", {})
    return predicate.get("Logical", "-")


def trigger_conditions(trigger: dict) -> list[str]:
    predicate = trigger.get("Predicate", {})
    conditions = predicate.get("Conditions", [])

    watched = []

    for condition in conditions:
        job_name = condition.get("JobName")
        crawler_name = condition.get("CrawlerName")
        state = condition.get("State", "-")

        if job_name:
            watched.append(f"`{job_name}` `{state}`")
        elif crawler_name:
            watched.append(f"`{crawler_name}` `{state}`")

    return watched


def trigger_actions(trigger: dict) -> list[str]:
    actions = trigger.get("Actions", [])

    starts = []

    for action in actions:
        job_name = action.get("JobName")
        crawler_name = action.get("CrawlerName")

        if job_name:
            starts.append(f"`{job_name}`")
        elif crawler_name:
            starts.append(f"`{crawler_name}`")

    return starts


def extract_triggers(graph: dict) -> list[dict]:
    """
    Pull trigger details out of graph nodes.
    """
    trigger_rows = []

    for node in graph.get("Nodes", []):
        if node.get("Type") != "TRIGGER":
            continue

        trigger_details = node.get("TriggerDetails", {})
        trigger = trigger_details.get("Trigger", {})

        name = trigger.get("Name", node.get("Name", "-"))
        logic = trigger_logic(trigger)
        watched = trigger_conditions(trigger)
        starts = trigger_actions(trigger)

        trigger_rows.append(
            {
                "name": name,
                "logic": logic,
                "watched": watched,
                "starts": starts,
            }
        )

    return sorted(trigger_rows, key=lambda row: row["name"])


def markdown_trigger_table(trigger_rows: list[dict]) -> list[str]:
    lines = [
        "| Trigger | Logic | Watches | Starts |",
        "|---|---|---|---|",
    ]

    for row in trigger_rows:
        watched = "<br>".join(row["watched"]) if row["watched"] else "-"
        starts = "<br>".join(row["starts"]) if row["starts"] else "-"

        lines.append(
            f"| `{row['name']}` | `{row['logic']}` | {watched} | {starts} |"
        )

    return lines


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    with INPUT_PATH.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    workflow = get_workflow(payload)
    graph = get_graph(workflow)

    workflow_name = workflow.get("Name", "restaurant_daily_pipeline_workflow")

    mermaid_lines = extract_mermaid_from_edges(graph)
    trigger_rows = extract_triggers(graph)

    lines = [
        f"# Glue Workflow Orchestration: `{workflow_name}`",
        "",
        "This document summarizes the AWS Glue Workflow used to orchestrate the restaurant analytics pipeline.",
        "",
        "## Pipeline overview",
        "",
        "The workflow orchestrates movement through the following layers:",
        "",
        "```text",
        "SQL Server → S3 raw → S3 silver → S3 gold → S3 marts",
        "```",
        "",
        "## Workflow diagram",
        "",
        *mermaid_lines,
        "",
        "## Trigger dependency table",
        "",
        *markdown_trigger_table(trigger_rows),
        "",
        "## Notes",
        "",
        "- The exported Glue workflow JSON is stored at `docs/glue_workflow_restaurant_daily_pipeline.json`.",
        "- The Mermaid diagram and trigger table are generated from the exported workflow graph.",
        "- AWS Glue's console graph is useful for a high-level visual, but the JSON export is the source of truth for documentation.",
        "",
    ]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()