"""
Fleet Knowledge Graph
=====================
Builds an ephemeral NetworkX DiGraph each Flink tick that encodes the current
state of the EV fleet, charging stations, and time-slot reservations.

Graph topology
--------------
  car_{id}          ──(travel_cost_eur, distance_km)──>  station_{id}
  station_{id}      ──(projected_price)──────────────>  {station_id}:slot:{slot_idx}
  car_{id}          ──(relation='reserved')───────────>  {station_id}:slot:{slot_idx}

Node types
----------
  type='car'      x, y, soc, emergency
  type='station'  x, y, base_price, capacity, active_chargers
  type='slot'     station_id, slot_idx, reservation_count, projected_price, is_full

No Flink / Kafka imports — pure Python + NetworkX.
"""

import math
import json
from typing import Dict, List, Optional, Tuple

import networkx as nx
from networkx.readwrite import node_link_data as _node_link_data

import sys, os
_cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config")
if _cfg_path not in sys.path:
    sys.path.insert(0, _cfg_path)

from stations import (
    STATIONS, PRICE_LEVELS, ENERGY_PER_SLOT,
    CONSUMPTION_KWH_PER_KM, ROUND_TRIP_FACTOR,
)

SLOTS_PER_DAY = 96


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _euclidean_km(x1: float, y1: float, x2: float, y2: float) -> float:
    return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)


def _travel_cost_eur(distance_km: float, base_price: float) -> float:
    """Round-trip energy cost in EUR: 2 × dist × 0.2 kWh/km × base_price / 1000."""
    return ROUND_TRIP_FACTOR * distance_km * CONSUMPTION_KWH_PER_KM * base_price / 1000.0


def _projected_price(reservation_count: int, capacity: int, base_price: float) -> float:
    """State-based slot price given existing reservation count at a station."""
    ratio = reservation_count / max(capacity, 1)
    for upper, mult in PRICE_LEVELS:
        if ratio <= upper:
            return base_price * mult
    return base_price * PRICE_LEVELS[-1][1]


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_fleet_graph(
    cars: Dict[str, dict],
    station_reservation_counts: Dict[str, Dict[int, int]],
    current_idx: int,
    car_reservations: Optional[Dict[str, dict]] = None,
) -> nx.DiGraph:
    """
    Build the ephemeral fleet planning graph for one tick.

    Parameters
    ----------
    cars                       : {car_id: car_data_dict}
                                 Must contain 'x', 'y', 'current_soc'.
    station_reservation_counts : {station_id: {slot_idx: count}}
    current_idx                : current simulation slot index (0-95)
    car_reservations           : optional {car_id: {station_id, reserved_slots, ...}}
                                 Used to add car→slot RESERVED edges for Phase 2.

    Returns
    -------
    nx.DiGraph  — ephemeral; never stored in Flink state.
    """
    G = nx.DiGraph()

    # ── Station nodes ──────────────────────────────────────────────────────
    for sid, stn in STATIONS.items():
        G.add_node(
            sid,
            type="station",
            name=stn["name"],
            x=stn["x"],
            y=stn["y"],
            base_price=stn["base_price"],
            capacity=stn["capacity"],
            active_chargers=0,   # updated by caller if available
        )

    # ── Slot nodes and station→slot edges ─────────────────────────────────
    for sid, stn in STATIONS.items():
        counts = station_reservation_counts.get(sid, {})
        for slot_idx in range(SLOTS_PER_DAY):
            count    = counts.get(slot_idx, 0)
            price    = _projected_price(count, stn["capacity"], stn["base_price"])
            is_full  = count >= stn["capacity"]
            node_id  = f"{sid}:slot:{slot_idx}"
            G.add_node(
                node_id,
                type="slot",
                station_id=sid,
                slot_idx=slot_idx,
                reservation_count=count,
                projected_price=price,
                is_full=is_full,
            )
            # station → slot  (weight = projected_price for this slot)
            G.add_edge(sid, node_id, weight=price)

    # ── Car nodes and car→station edges ───────────────────────────────────
    for car_id, car_data in cars.items():
        cx  = car_data.get("x", 5.0)
        cy  = car_data.get("y", 5.0)
        soc = car_data.get("current_soc", 1.0)
        G.add_node(
            car_id,
            type="car",
            x=cx,
            y=cy,
            soc=soc,
            emergency=soc < 0.15,
        )
        for sid, stn in STATIONS.items():
            dist_km  = _euclidean_km(cx, cy, stn["x"], stn["y"])
            t_cost   = _travel_cost_eur(dist_km, stn["base_price"])
            G.add_edge(
                car_id,
                sid,
                weight=t_cost,
                distance_km=dist_km,
                travel_cost_eur=t_cost,
            )

    # ── RESERVED edges (car → slot) ───────────────────────────────────────
    if car_reservations:
        for car_id, res in car_reservations.items():
            if car_id not in G:
                continue
            sid   = res.get("station_id")
            slots = res.get("reserved_slots", [])
            for slot_idx in slots:
                node_id = f"{sid}:slot:{slot_idx}"
                if G.has_node(node_id):
                    G.add_edge(car_id, node_id, relation="reserved")

    return G


# ---------------------------------------------------------------------------
# Planning queries
# ---------------------------------------------------------------------------

def select_station_and_slots(
    G: nx.DiGraph,
    car_id: str,
    num_needed: int,
    current_idx: int,
) -> Tuple[Optional[str], List[int]]:
    """
    Normal car routing: find (station, slots) that minimises total_EUR.

        total_EUR = travel_cost_eur(car → station)
                  + Σ projected_price(slot) × ENERGY_PER_SLOT / 1000
                    for the cheapest num_needed future slots at that station.

    Algorithm: greedy bipartite selection — no NP-hard combinatorics.
    Returns (station_id, sorted_slot_list) or (None, []) if nothing feasible.
    """
    if car_id not in G:
        return None, []

    best_station: Optional[str] = None
    best_slots:   List[int]     = []
    best_total:   float         = float("inf")

    for _, station_id, edge_data in G.out_edges(car_id, data=True):
        if G.nodes[station_id].get("type") != "station":
            continue

        travel_eur = edge_data["travel_cost_eur"]

        # Gather future non-full slot options at this station
        candidates: List[Tuple[float, int]] = []
        for _, slot_node_id, slot_edge in G.out_edges(station_id, data=True):
            slot_node = G.nodes[slot_node_id]
            if slot_node.get("type") != "slot":
                continue
            if slot_node["slot_idx"] < current_idx:
                continue
            if slot_node["is_full"]:
                continue
            candidates.append((slot_edge["weight"], slot_node["slot_idx"]))

        if len(candidates) < num_needed:
            continue

        # Pick cheapest num_needed slots
        candidates.sort(key=lambda x: (x[0], x[1]))
        chosen = candidates[:num_needed]

        charging_eur = sum(price * ENERGY_PER_SLOT / 1000.0 for price, _ in chosen)
        total        = travel_eur + charging_eur

        if total < best_total:
            best_total   = total
            best_station = station_id
            best_slots   = sorted(slot_idx for _, slot_idx in chosen)

    return best_station, best_slots


def nearest_available_station(
    G: nx.DiGraph,
    car_id: str,
    current_idx: int,
) -> Optional[str]:
    """
    Emergency car routing: return the nearest station (minimum distance_km)
    that has at least one non-full slot at current_idx.
    Ignores price — time-to-charge is the only objective for emergencies.
    Returns None if every station is full at current_idx.
    """
    if car_id not in G:
        return None

    best_station:  Optional[str] = None
    best_distance: float         = float("inf")

    for _, station_id, edge_data in G.out_edges(car_id, data=True):
        if G.nodes[station_id].get("type") != "station":
            continue
        slot_node_id = f"{station_id}:slot:{current_idx}"
        if not G.has_node(slot_node_id):
            continue
        if G.nodes[slot_node_id]["is_full"]:
            continue
        dist = edge_data["distance_km"]
        if dist < best_distance:
            best_distance = dist
            best_station  = station_id

    return best_station


def get_cars_reserved_at_slot(
    G: nx.DiGraph,
    slot_idx: int,
) -> Dict[str, str]:
    """
    Return {car_id: station_id} for every car that has a RESERVED edge
    pointing to the given slot index (any station).
    Used in Phase 2 to build the reserved_this_slot lookup.
    """
    result: Dict[str, str] = {}
    for node_id, node_data in G.nodes(data=True):
        if node_data.get("type") != "car":
            continue
        for _, slot_node_id, edge_data in G.out_edges(node_id, data=True):
            if edge_data.get("relation") != "reserved":
                continue
            slot_node = G.nodes.get(slot_node_id, {})
            if slot_node.get("slot_idx") == slot_idx:
                result[node_id] = slot_node["station_id"]
                break
    return result


def get_reservation_summary(G: nx.DiGraph) -> Dict[str, Dict[int, int]]:
    """
    Extract {station_id: {slot_idx: reservation_count}} from slot node attributes.
    Used to build the RESERVATION_STATUS message.
    """
    summary: Dict[str, Dict[int, int]] = {}
    for node_id, node_data in G.nodes(data=True):
        if node_data.get("type") != "slot":
            continue
        count = node_data.get("reservation_count", 0)
        if count > 0:
            sid      = node_data["station_id"]
            slot_idx = node_data["slot_idx"]
            summary.setdefault(sid, {})[slot_idx] = count
    return summary


# ---------------------------------------------------------------------------
# Academic / debug utilities
# ---------------------------------------------------------------------------

def snapshot_graph(G: nx.DiGraph) -> str:
    """
    Serialise the graph to a JSON string using NetworkX node-link format.
    NOT called in the hot path — use only for debugging or academic analysis.
    Reconstruct with: nx.node_link_graph(json.loads(snapshot))
    """
    data = _node_link_data(G)
    return json.dumps(data, default=float)
