# coding: utf-8

import string, re


def str2sec(info):
    """ Parses a string describing somehow a time span to seconds:
    1 sec -> 1
    1 min -> 60
    1 hour -> 3600
    1 day ->
    1 week ->
    1 month -> (30 days)
    1 year -> (355 days)
    """
    def is_number(s):
        try:
            float(s)
            return True
        except:
            return False

    out = 0

    info = info.split()
    unit = None
    value = 0
    for part in info:
        if is_number(part):
            value = float(part)
        else:
            unit = part

            if unit.startswith("sec"):
                out += value
            elif unit.startswith("min"):
                out += value * 60
            elif unit.startswith("hour"):
                out += value * 60 * 60
            elif unit.startswith("day"):
                out += value * 60 * 60 * 24
            elif unit.startswith("week"):
                out += value * 60 * 60 * 24 * 7
            elif unit.startswith("month"):
                out += value * 60 * 60 * 24 * 30
            elif unit.startswith("year"):
                out += value * 60 * 60 * 24 * 355

    return int(out)

def parse_plan(tag, plan, config, idx):
    plan_info = {"tag": tag,
                 "idx": idx,
                 "min": None,
                 "hour": None,
                 "wday": None,
                 "urls": [],
                 "proxy_port": int(config.get("proxy", "proxy_port")) + idx,
                 "label": "unnamed test",
                 "delay": 0,
                 "screenshot": False,
                 "keep_data": str2sec(config.get("config", "keep_data")),
                 "keep_screenshots": str2sec(config.get("config", "keep_screenshots")),
                 "keep_orphaned_screenshots": str2sec(config.get("config", "keep_orphaned")),
                 "timeout": 30,
                 "retries": 0,
                 "priority": "normal"
                 }

    idx = 0
    for el in plan.split("\n"):

        el = string.strip(el)

        if el.startswith("label "):
            plan_info["label"] = string.strip(el[6:])
        elif el.startswith("every "):
            when = el[6:].split()
            if when[1].startswith("min"):
                plan_info["min"] = int(when[0])
            elif when[1].startswith("hour"):
                plan_info["hour"] = int(when[0])
            elif when[1].startswith("wday"):
                plan_info["wday"] = int(when[0])
        elif el.startswith("http"):
            plan_info["urls"].append(el)
        elif el.startswith("("):
            visual, url = re.match("\((.*?)\)\s*?(.*)", el).groups()
            plan_info["urls"].append((url, visual))
        elif el.startswith("screenshot"):
            plan_info["screenshot"] = True
        elif el.startswith("proxy_port"):
            plan_info["proxy_port"] = int(el.split()[-1]) + idx
        elif el.startswith("delay"):
            plan_info["delay"] = int(el.split()[-1])
        elif el.startswith("keep data "):
            plan_info["keep_data"] = str2sec(el[10:])
        elif el.startswith("keep screenshots "):
            plan_info["keep_screenshots"] = str2sec(el[17:])
        elif el.startswith("keep orphaned screenshots "):
            plan_info["keep_orphaned_screenshots"] = str2sec(el[26:])
        elif el.startswith("script"):
            plan_info["urls"].append(el)
        elif el.startswith("timeout "):
            plan_info["timeout"] = int(el[8:])
        elif el.startswith("retries "):
            plan_info["retries"] = int(el[8:])
        elif el.startswith("priority "):
            plan_info["priority"] = string.strip(el[9:])


    return plan_info
