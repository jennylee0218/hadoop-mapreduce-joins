"""Part 1: word count mapper, reducer, and the reducer used as a combiner.

Hand-checked sample. Three lines, lowercased, words = [a-z']+ :
    "the data the"      -> the, data, the
    "The Data pipeline" -> the, data, pipeline
    "don't stop; THE"   -> don't, stop, the
Totals: the=4  data=2  pipeline=1  don't=1  stop=1   (9 word occurrences)
"""
from conftest import run_script, shuffle_sort

SAMPLE = "the data the\nThe Data pipeline\ndon't stop; THE\n"
EXPECTED = {"the": 4, "data": 2, "pipeline": 1, "don't": 1, "stop": 1}


def parse(tsv):
    return {k: int(v) for k, v in (ln.split("\t") for ln in tsv.splitlines())}


def test_mapper_emits_one_pair_per_occurrence():
    out = run_script("wordcount/mapper.py", SAMPLE).splitlines()
    assert len(out) == 9, "the mapper must not pre-aggregate — Part 1c compares it against the combiner"
    assert all(ln.endswith("\t1") for ln in out)
    assert out[0] == "the\t1"


def test_reducer_sums_each_word():
    mapped = run_script("wordcount/mapper.py", SAMPLE)
    reduced = run_script("wordcount/reducer.py", shuffle_sort(mapped))
    assert parse(reduced) == EXPECTED


def test_reducer_is_a_legal_combiner():
    """mapper | sort | reducer | sort | reducer must equal mapper | sort | reducer.
    That is what makes -combiner reducer.py safe: sum is associative and commutative."""
    mapped = run_script("wordcount/mapper.py", SAMPLE)
    combined = run_script("wordcount/reducer.py", shuffle_sort(mapped))
    final = run_script("wordcount/reducer.py", shuffle_sort(combined))
    assert parse(final) == EXPECTED


def test_reducer_adds_incoming_counts_not_ones():
    """Partial counts arrive from the combiner, so the reducer must read the value."""
    reduced = run_script("wordcount/reducer.py", "shuffle\t5\nshuffle\t7\nspark\t3\n")
    assert parse(reduced) == {"shuffle": 12, "spark": 3}


def test_word_total_equals_corpus_word_count():
    """Sum of all reducer outputs == number of word occurrences in the input."""
    mapped = run_script("wordcount/mapper.py", SAMPLE)
    reduced = run_script("wordcount/reducer.py", shuffle_sort(mapped))
    assert sum(parse(reduced).values()) == 9
