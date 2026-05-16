"""VPS provider catalog.

Curated set of plans across Hetzner, DigitalOcean, Linode, Vultr that map
cleanly to UDP's three sizing tiers. Pricing is approximate USD/month as of
early 2026; the UI labels these as estimates with a "verify on provider"
note. Every entry includes a direct signup/pricing link.

The Studio surfaces the cheapest plan per provider that *fits* each tier,
plus a "cheapest overall" recommendation.
"""
from __future__ import annotations
from typing import Any

from .sizer import fits


# ----- Provider catalog -----
# Fields:
#   id, plan, cpu, ram_gb, disk_gb (NVMe/SSD included with plan), traffic_tb,
#   usd_month (approx), location_examples, url

HETZNER: list[dict[str, Any]] = [
    {"id": "hetzner-ccx13", "plan": "CCX13 (dedicated AMD)", "cpu": 2,  "ram_gb": 8,  "disk_gb": 80,  "traffic_tb": 20, "usd_month": 14, "location": "Falkenstein / Helsinki / Ashburn"},
    {"id": "hetzner-ccx23", "plan": "CCX23",                "cpu": 4,  "ram_gb": 16, "disk_gb": 160, "traffic_tb": 20, "usd_month": 28, "location": "Falkenstein / Helsinki / Ashburn"},
    {"id": "hetzner-ccx33", "plan": "CCX33",                "cpu": 8,  "ram_gb": 32, "disk_gb": 240, "traffic_tb": 30, "usd_month": 56, "location": "Falkenstein / Helsinki / Ashburn"},
    {"id": "hetzner-ccx43", "plan": "CCX43",                "cpu": 16, "ram_gb": 64, "disk_gb": 360, "traffic_tb": 40, "usd_month": 112, "location": "Falkenstein / Helsinki / Ashburn"},
    {"id": "hetzner-ccx53", "plan": "CCX53",                "cpu": 32, "ram_gb": 128, "disk_gb": 600, "traffic_tb": 50, "usd_month": 224, "location": "Falkenstein / Helsinki / Ashburn"},
]
HETZNER_URL = "https://www.hetzner.com/cloud"

DIGITALOCEAN: list[dict[str, Any]] = [
    {"id": "do-s-4vcpu-8gb",   "plan": "Basic Premium AMD 4 vCPU / 8 GB",   "cpu": 4,  "ram_gb": 8,  "disk_gb": 160, "traffic_tb": 5, "usd_month": 56, "location": "NYC / SFO / FRA / BLR / SGP"},
    {"id": "do-s-4vcpu-16gb",  "plan": "Basic Premium AMD 4 vCPU / 16 GB",  "cpu": 4,  "ram_gb": 16, "disk_gb": 320, "traffic_tb": 6, "usd_month": 96, "location": "NYC / SFO / FRA / BLR / SGP"},
    {"id": "do-s-8vcpu-32gb",  "plan": "General Purpose 8 vCPU / 32 GB",    "cpu": 8,  "ram_gb": 32, "disk_gb": 200, "traffic_tb": 6, "usd_month": 240, "location": "NYC / SFO / FRA / BLR / SGP"},
    {"id": "do-s-16vcpu-64gb", "plan": "General Purpose 16 vCPU / 64 GB",   "cpu": 16, "ram_gb": 64, "disk_gb": 400, "traffic_tb": 9, "usd_month": 480, "location": "NYC / SFO / FRA / BLR / SGP"},
]
DIGITALOCEAN_URL = "https://www.digitalocean.com/pricing/droplets"

LINODE: list[dict[str, Any]] = [
    {"id": "linode-d4",  "plan": "Dedicated 8 GB",  "cpu": 4,  "ram_gb": 8,  "disk_gb": 160, "traffic_tb": 5, "usd_month": 72,  "location": "Mumbai / Singapore / Frankfurt / Newark"},
    {"id": "linode-d6",  "plan": "Dedicated 16 GB", "cpu": 8,  "ram_gb": 16, "disk_gb": 320, "traffic_tb": 6, "usd_month": 144, "location": "Mumbai / Singapore / Frankfurt / Newark"},
    {"id": "linode-d10", "plan": "Dedicated 32 GB", "cpu": 16, "ram_gb": 32, "disk_gb": 640, "traffic_tb": 7, "usd_month": 288, "location": "Mumbai / Singapore / Frankfurt / Newark"},
    {"id": "linode-d14", "plan": "Dedicated 64 GB", "cpu": 32, "ram_gb": 64, "disk_gb": 1280, "traffic_tb": 8, "usd_month": 576, "location": "Mumbai / Singapore / Frankfurt / Newark"},
]
LINODE_URL = "https://www.linode.com/pricing/"

VULTR: list[dict[str, Any]] = [
    {"id": "vultr-hf-4-16",  "plan": "High Performance 4 vCPU / 16 GB",  "cpu": 4,  "ram_gb": 16, "disk_gb": 320, "traffic_tb": 5, "usd_month": 96,  "location": "Bangalore / Mumbai / Tokyo / FRA / NYC"},
    {"id": "vultr-hf-8-32",  "plan": "High Performance 8 vCPU / 32 GB",  "cpu": 8,  "ram_gb": 32, "disk_gb": 640, "traffic_tb": 6, "usd_month": 192, "location": "Bangalore / Mumbai / Tokyo / FRA / NYC"},
    {"id": "vultr-hf-16-64", "plan": "High Performance 16 vCPU / 64 GB", "cpu": 16, "ram_gb": 64, "disk_gb": 1280, "traffic_tb": 7, "usd_month": 384, "location": "Bangalore / Mumbai / Tokyo / FRA / NYC"},
]
VULTR_URL = "https://www.vultr.com/products/cloud-compute/"

PROVIDERS = [
    {"id": "hetzner",      "name": "Hetzner Cloud",   "url": HETZNER_URL,      "plans": HETZNER,
     "notes": "Cheapest for the dedicated-CPU tier; EU + 1 US region; no India region."},
    {"id": "digitalocean", "name": "DigitalOcean",    "url": DIGITALOCEAN_URL, "plans": DIGITALOCEAN,
     "notes": "Bangalore region available; well-documented; easy snapshots."},
    {"id": "linode",       "name": "Akamai Linode",   "url": LINODE_URL,       "plans": LINODE,
     "notes": "Mumbai region available; predictable pricing."},
    {"id": "vultr",        "name": "Vultr",           "url": VULTR_URL,        "plans": VULTR,
     "notes": "Bangalore + Mumbai regions; High-Frequency plans are a good fit."},
]


def match_plans(totals: dict, *, limit_per_provider: int = 1) -> list[dict]:
    """For each provider, return the cheapest plan(s) that fit `totals`."""
    out = []
    for prov in PROVIDERS:
        candidates = [p for p in prov["plans"] if fits(totals, p)]
        candidates.sort(key=lambda p: p["usd_month"])
        chosen = candidates[:limit_per_provider]
        out.append({
            "provider_id": prov["id"],
            "provider_name": prov["name"],
            "provider_url": prov["url"],
            "notes": prov["notes"],
            "fitting_plans": chosen,
            "no_fit": len(chosen) == 0,
        })
    return out


def cheapest_overall(totals: dict) -> dict | None:
    """Return the single cheapest fitting plan across all providers, or None."""
    candidates = []
    for prov in PROVIDERS:
        for p in prov["plans"]:
            if fits(totals, p):
                candidates.append({**p, "provider_id": prov["id"], "provider_name": prov["name"], "provider_url": prov["url"]})
    if not candidates:
        return None
    candidates.sort(key=lambda p: p["usd_month"])
    return candidates[0]
