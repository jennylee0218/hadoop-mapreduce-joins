"""Part 4.2: the salted reduce-side join must give byte-identical results to the unsalted one.

Same hand-checked sample as test_joins.py:
    customers: 1 Ada Lovelace / 2 Grace Hopper / 3 Alan Turing
    orders:    1 -> 10.00, 2.50, 7.25   2 -> 100.00   99 -> 5.00 (no such customer)
    expected:  1 -> (3, 19.75)   2 -> (1, 100.00)   3 -> (0, 0.00)

Salts are chosen at random, so nothing here asserts a per-salt distribution — only that
the totals survive the round trip and that no name goes missing.
"""
import pytest
from conftest import run_script, shuffle_sort
from test_joins import CUSTOMERS, ORDERS, rows

SALTS = 10


def salted_pipeline(customers=CUSTOMERS, orders=ORDERS, replicate_customers=True):
    """mapper -> sort -> step-1 reducer -> sort -> merge reducer, as Hadoop would run it."""
    mapped = run_script("joins/salted_mapper.py", orders,
                        {"mapreduce_map_input_file": "orders.csv"})
    cust_out = run_script("joins/salted_mapper.py", customers,
                          {"mapreduce_map_input_file": "customers.csv"})
    if not replicate_customers:
        # The classic mistake the brief warns about: salt the big side, send each
        # customers row to salt 0 only instead of to every salt.
        cust_out = "".join(l + "\n" for l in cust_out.splitlines() if l.split("\t")[0].endswith("#0"))
    partials = run_script("joins/salted_reducer.py", shuffle_sort(mapped + cust_out))
    return run_script("joins/merge_reducer.py", shuffle_sort(partials)), partials


def test_salted_join_matches_the_unsalted_join():
    merged, _ = salted_pipeline()
    assert rows(merged) == {
        "1": ("Ada Lovelace", 3, 19.75),
        "2": ("Grace Hopper", 1, 100.00),
        "3": ("Alan Turing", 0, 0.00),
    }


def test_every_customer_keeps_its_name():
    """The failure mode is silent: orders survive, names vanish."""
    merged, _ = salted_pipeline()
    for cid, (name, _, _) in rows(merged).items():
        assert name and name != "", f"customer {cid} lost its name"


def test_customers_are_replicated_across_every_salt():
    out = run_script("joins/salted_mapper.py", CUSTOMERS,
                     {"mapreduce_map_input_file": "customers.csv"})
    keys = [l.split("\t")[0] for l in out.splitlines()]
    assert len(keys) == 3 * SALTS, "each customers row must be emitted once per salt"
    assert sorted(k.split("#")[1] for k in keys if k.startswith("1#")) == \
           sorted(str(s) for s in range(SALTS))


def test_orders_are_salted_but_not_replicated():
    out = run_script("joins/salted_mapper.py", ORDERS,
                     {"mapreduce_map_input_file": "orders.csv"})
    assert len(out.splitlines()) == 5, "one row per order, no duplication"
    assert all("#" in l.split("\t")[0] for l in out.splitlines())


def test_forgetting_to_replicate_customers_is_caught():
    """Proof the guard works: break it on purpose and the totals must break too."""
    merged, _ = salted_pipeline(replicate_customers=False)
    broken = rows(merged)
    good = {"1": ("Ada Lovelace", 3, 19.75), "2": ("Grace Hopper", 1, 100.00)}
    assert broken != good, "the bad pipeline produced correct output -- the test is blind"
    lost = sum(good[c][1] - broken.get(c, ("", 0, 0))[1] for c in good)
    assert lost > 0, "orders should have been dropped when names went missing"


def test_partials_carry_more_than_two_decimals():
    """Rounding each partial to cents would drift once ten of them are summed."""
    _, partials = salted_pipeline()
    assert any(len(l.split("\t")[3].split(".")[1]) > 2 for l in partials.splitlines())
