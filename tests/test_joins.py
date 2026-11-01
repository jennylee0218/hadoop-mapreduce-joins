"""Part 2: reduce-side join and map-side (broadcast) join.

Hand-checked sample — 3 customers, 5 orders, one customer with no orders,
one order belonging to a customer we were not given (id 99):

    customers: 1 Ada Lovelace / 2 Grace Hopper / 3 Alan Turing
    orders:    1 -> 10.00, 2.50, 7.25   2 -> 100.00   99 -> 5.00

    customer 1: n=3  total=19.75
    customer 2: n=1  total=100.00
    customer 3: n=0  total=0.00      (reduce-side prints it, map-side cannot)
    customer 99: dropped by both — no name to join to
"""
import pytest
from conftest import run_script, shuffle_sort

CUSTOMERS = (
    "customer_id,name,state,segment\n"
    "1,Ada Lovelace,IL,consumer\n"
    "2,Grace Hopper,NY,smb\n"
    "3,Alan Turing,CA,enterprise\n"
)
ORDERS = (
    "order_id,customer_id,order_date,amount\n"
    "1,1,2025-01-05,10.00\n"
    "2,2,2025-01-06,100.00\n"
    "3,1,2025-02-11,2.50\n"
    "4,99,2025-02-12,5.00\n"
    "5,1,2025-03-01,7.25\n"
)


def rows(tsv):
    return {f[0]: (f[1], int(f[2]), float(f[3]))
            for f in (ln.split("\t") for ln in tsv.splitlines())}


def map_both_sides():
    """Both files go through the same mapper; only the env var distinguishes them."""
    return (
        run_script("joins/reduce_side_mapper.py", CUSTOMERS,
                   {"mapreduce_map_input_file": "hdfs://nn/a1/joins/customers.csv"})
        + run_script("joins/reduce_side_mapper.py", ORDERS,
                     {"mapreduce_map_input_file": "hdfs://nn/a1/joins/orders.csv"})
    )


def test_mapper_tags_each_source():
    out = map_both_sides().splitlines()
    assert "1\tC,Ada Lovelace" in out
    assert "1\tO,10.00" in out
    assert "99\tO,5.00" in out
    assert not any(ln.startswith("customer_id") or ln.startswith("order_id") for ln in out)


def test_reduce_side_join():
    out = rows(run_script("joins/reduce_side_reducer.py", shuffle_sort(map_both_sides())))
    assert out == {
        "1": ("Ada Lovelace", 3, 19.75),
        "2": ("Grace Hopper", 1, 100.00),
        "3": ("Alan Turing", 0, 0.00),
    }
    assert "99" not in out, "an order with no matching customer must not produce a row"


def test_reduce_side_join_when_customer_record_arrives_last():
    """Hadoop does not promise value order inside a group. Orders first must still work."""
    stream = "1\tO,10.00\n1\tO,2.50\n1\tO,7.25\n1\tC,Ada Lovelace\n"
    assert rows(run_script("joins/reduce_side_reducer.py", stream)) == {
        "1": ("Ada Lovelace", 3, 19.75)
    }


def test_mappers_tolerate_crlf():
    """make_data.py uses csv.writer, whose default line terminator is \r\n.
    A mapper that strips only \n leaks a carriage return into the HDFS output."""
    crlf_orders = ORDERS.replace("\n", "\r\n")
    out = run_script("joins/reduce_side_mapper.py", crlf_orders,
                     {"mapreduce_map_input_file": "orders.csv"})
    assert "\r" not in out, "carriage return leaked into the shuffle value"
    assert "1\tO,10.00" in out.splitlines()


def test_map_side_mapper_tolerates_crlf(tmp_path):
    (tmp_path / "customers.csv").write_text(CUSTOMERS.replace("\n", "\r\n"))
    out = run_script("joins/map_side_mapper.py", ORDERS.replace("\n", "\r\n"), cwd=tmp_path)
    assert "\r" not in out, "carriage return leaked into the job output"
    assert out.splitlines()[0] == "1|Ada Lovelace\t10.00"


def test_map_side_mapper(tmp_path):
    """customers.csv arrives via -files, i.e. in the task's working directory."""
    (tmp_path / "customers.csv").write_text(CUSTOMERS)
    out = run_script("joins/map_side_mapper.py", ORDERS, cwd=tmp_path).splitlines()
    assert out == [
        "1|Ada Lovelace\t10.00",
        "2|Grace Hopper\t100.00",
        "1|Ada Lovelace\t2.50",
        "1|Ada Lovelace\t7.25",
    ], "one row per order, in order, customer 99 dropped"


def test_both_joins_agree_on_totals(tmp_path):
    """The two joins must reconcile wherever the map-side job can produce a row."""
    reduce_side = rows(run_script("joins/reduce_side_reducer.py", shuffle_sort(map_both_sides())))

    (tmp_path / "customers.csv").write_text(CUSTOMERS)
    map_side = {}
    for ln in run_script("joins/map_side_mapper.py", ORDERS, cwd=tmp_path).splitlines():
        key, amount = ln.split("\t")
        cid, name = key.split("|")
        n, total = map_side.get(cid, (0, 0.0))
        map_side[cid] = (n + 1, total + float(amount))

    for cid, (n, total) in map_side.items():
        assert reduce_side[cid][1] == n
        assert reduce_side[cid][2] == pytest.approx(total, abs=0.01)
    # the only row the map-side job is missing is the customer with zero orders
    assert set(reduce_side) - set(map_side) == {"3"}
