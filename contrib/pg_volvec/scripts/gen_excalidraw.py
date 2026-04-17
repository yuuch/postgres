import json
import uuid

def make_id(): return str(uuid.uuid4())

elements = []

def create_node(text, x, y, stroke_color="#000000", bg_color="transparent"):
    rect_id = make_id()
    text_id = make_id()
    lines = text.split("\n")
    max_len = max(len(line) for line in lines)
    width = max(240, max_len * 9 + 40)
    height = max(100, len(lines) * 22 + 40)
    
    rect = {
        "id": rect_id, "type": "rectangle", "x": x, "y": y,
        "width": width, "height": height,
        "strokeColor": stroke_color, "backgroundColor": bg_color,
        "fillStyle": "solid", "strokeWidth": 2, "strokeStyle": "solid",
        "roughness": 0, "opacity": 100, "groupIds": [], "roundness": {"type": 3},
        "boundElements": [{"id": text_id, "type": "text"}],
        "seed": 1, "version": 1, "versionNonce": 1, "isDeleted": False
    }
    
    text_el = {
        "id": text_id, "type": "text", "x": x + 20, "y": y + 20,
        "width": width - 40, "height": height - 40,
        "strokeColor": stroke_color, "text": text,
        "fontSize": 16, "fontFamily": 2, "textAlign": "center",
        "verticalAlign": "middle", "containerId": rect_id,
        "seed": 1, "version": 1, "versionNonce": 1, "isDeleted": False
    }
    elements.extend([rect, text_el])
    return rect_id, width, height

def create_arrow(start_id, start_x, start_y, end_id, end_x, end_y, stroke_style="solid", stroke_color="#000000", label=None):
    arrow_id = make_id()
    dx, dy = end_x - start_x, end_y - start_y
    arrow = {
        "id": arrow_id, "type": "arrow", "x": start_x, "y": start_y,
        "width": abs(dx) or 1, "height": abs(dy) or 1,
        "strokeColor": stroke_color, "backgroundColor": "transparent",
        "fillStyle": "hachure", "strokeWidth": 2, "strokeStyle": stroke_style,
        "roughness": 0, "opacity": 100, "groupIds": [], "roundness": {"type": 2},
        "points": [[0, 0], [dx, dy]],
        "startBinding": {"elementId": start_id, "focus": 0, "gap": 1},
        "endBinding": {"elementId": end_id, "focus": 0, "gap": 1},
        "endArrowhead": "arrow",
        "seed": 1, "version": 1, "versionNonce": 1, "isDeleted": False
    }
    elements.append(arrow)
    if label:
        text_id = make_id()
        elements.append({
            "id": text_id, "type": "text", "x": start_x + dx/2 - 40, "y": start_y + dy/2 - 10,
            "width": 100, "height": 20, "strokeColor": stroke_color, "text": label,
            "fontSize": 14, "fontFamily": 2, "textAlign": "center",
            "verticalAlign": "middle", "containerId": arrow_id,
            "seed": 1, "version": 1, "versionNonce": 1, "isDeleted": False
        })

subplans_to_draw = []

def layout_tree(node, x, y):
    child_x = x
    total_children_width = 0
    child_results = []
    
    for child in node.get("children", []):
        w, c_id, c_cx, c_by, c_top_y = layout_tree(child, child_x, y + 220)
        child_results.append((c_id, c_cx, c_top_y))
        child_x += w + 120
        total_children_width += w + 120
    
    if total_children_width > 0: total_children_width -= 120
    
    lines = node["text"].split("\n")
    node_w_est = max(240, max(len(l) for l in lines) * 9 + 40)
    final_width = max(total_children_width, node_w_est)
    
    node_x = x + (final_width - node_w_est) / 2
    n_id, n_w, n_h = create_node(node["text"], node_x, y, node.get("stroke", "#000000"), node.get("bg", "transparent"))
    n_cx = node_x + n_w / 2
    
    for c_id, c_cx, c_top_y in child_results:
        create_arrow(n_id, n_cx, y + n_h, c_id, c_cx, c_top_y)
        
    if "subplan_name" in node:
        subplans_to_draw.append({"caller_id": n_id, "caller_x": node_x + n_w, "caller_y": y + n_h/2, "subplan": node["subplan_data"]})
            
    return final_width, n_id, n_cx, y + n_h, y

# --- Data Structures ---
pg_main_tree = {
    "text": "PostgreSQL Main Execution Flow\n[Standard Row-based Tree]",
    "bg": "#fbe9e7", "stroke": "#d84315",
    "children": [
        {
            "text": "Sort\n[Rows: 1]",
            "children": [
                {
                    "text": "Hash Join (Top)\nCond: ((SubPlan 1) = ps_supplycost)\n[Rows: 1]",
                    "bg": "#fff9c4", "stroke": "#fbc02d",
                    "subplan_name": "SubPlan 1",
                    "subplan_data": {
                        "text": "SubPlan 1 (Correlated Subquery)\nCalled for every outer row",
                        "bg": "#ffebee", "stroke": "#c62828",
                        "children": [
                            {
                                "text": "Aggregate (min)\n[Rows: 1]",
                                "children": [
                                    {
                                        "text": "-> Hash Join (Inner)\n[Rows: 4]",
                                        "children": [
                                            {"text": "-> Seq Scan: partsupp\nFilter: ps_partkey = (outer) part.pk\n[Rows: 18]"},
                                            {"text": "-> Hash (Supp/Nat/Reg Subtree)\n[Rows: 20k]"}
                                        ]
                                    }
                                ]
                            }
                        ]
                    },
                    "children": [
                        {
                            "text": "-> Gather (Parallel Collect)\n[Rows: 7,761]",
                            "children": [{"text": "-> Parallel Seq Scan: part\n[Rows: 1,940 x 4]"}]
                        },
                        {
                            "text": "-> Hash (Build Side)\n[Rows: 1,563,901]",
                            "children": [
                                {
                                    "text": "-> Hash Join (ps.sk = s.sk)\n[Rows: 1,563,901]",
                                    "children": [
                                        {"text": "-> Seq Scan: partsupp\n[Rows: 8.0M]"},
                                        {"text": "-> Hash (Supplier/Nation/Region Tree)\n[Rows: 20k]"}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]
}

# --- Execute Layout ---
# 1. Main Tree
main_w, _, _, _, _ = layout_tree(pg_main_tree, 0, 0)

# 2. SubPlans column (Place far to the right of main_w)
subplan_x = main_w + 1000
subplan_y = 300
for sp_info in subplans_to_draw:
    sw, sid, scx, sby, stop_y = layout_tree(sp_info["subplan"], subplan_x, subplan_y)
    # Long arrow from Main Tree to SubPlan Column
    create_arrow(sp_info["caller_id"], sp_info["caller_x"], sp_info["caller_y"], sid, subplan_x, stop_y + 40, stroke_style="dashed", stroke_color="#c62828", label="Calls")
    subplan_y += sby + 200

# Wrap in Excalidraw format
excalidraw_data = {
    "type": "excalidraw", "version": 2, "source": "https://excalidraw.com",
    "elements": elements, "appState": {"viewBackgroundColor": "#ffffff"}, "files": {}
}

with open("plans/q2_compare.excalidraw", "w") as f:
    json.dump(excalidraw_data, f, indent=2)
