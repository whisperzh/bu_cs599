import pytest
from assignment_12 import *

pathf="../data/friends.txt"
pathr="../data/movie_ratings.txt"
uid=10
mid=3
resPath="../data/res.txt"
def test_query1_pull():
    sf = Scan(filepath=pathf, outputs=None)
    sr = Scan(filepath=pathr, outputs=None)
    se1 = Select(inputs=[sf], predicate={"UID1": uid}, outputs=None)
    se2 = Select(inputs=[sr], predicate={"MID": mid}, outputs=None)
    join = Join(left_inputs=[se1], right_inputs=[se2], outputs=None, left_join_attribute="UID2",
                right_join_attribute="UID")
    proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"])
    groupby = GroupBy(inputs=[proj], outputs=None, key="", value="Rating", agg_gun="AVG")
    sink=Sink(inputs=[groupby],outputs=None,filepath=resPath)
    sink.get_next()
    assert sink.output is not None
    pass

def test_query1_push():
    sink = Sink(inputs=None, outputs=None, filepath=resPath)
    groupby = GroupBy(inputs=None, outputs=[sink], key="", value="Rating", agg_gun="AVG")
    proj = Project(inputs=None, outputs=[groupby], fields_to_keep=["Rating"])
    join = Join(left_inputs=None, right_inputs=None, outputs=[proj], left_join_attribute="UID2",
                right_join_attribute="UID")
    se1 = Select(inputs=None, predicate={"UID1": uid}, outputs=[join])
    sf = Scan(filepath=pathf, outputs=[se1])
    se2 = Select(inputs=None, predicate={"MID": mid},outputs=[join])
    sr = Scan(filepath=pathr, isleft=False, outputs=[se2])
    sf.start()
    sr.start()
    assert sink.output is not None
    pass

def test_query2_pull():
    sf = Scan(filepath=pathf, outputs=None)
    sr = Scan(filepath=pathr, outputs=None)
    se1 = Select(inputs=[sf], predicate={"UID1": uid}, outputs=None)
    se2 = Select(inputs=[sr], predicate=None, outputs=None)
    join = Join(left_inputs=[se1], right_inputs=[se2], outputs=None, left_join_attribute="UID2",
                right_join_attribute="UID")
    proj = Project(inputs=[join], outputs=None, fields_to_keep=["MID", "Rating"])
    groupby = GroupBy(inputs=[proj], outputs=None, key="MID", value="Rating", agg_gun="AVG")
    orderby = OrderBy(inputs=[groupby], outputs=None, comparator="Rating", ASC=False)
    topk = TopK(inputs=[orderby], outputs=None, k=1)
    sink = Sink(inputs=[topk], outputs=None, filepath=resPath)
    sink.get_next()
    assert sink.output is not None
    pass

def test_query2_push():
    sink = Sink(inputs=None, outputs=None, filepath=resPath)
    topk = TopK(inputs=None, outputs=[sink], k=1)
    orderby = OrderBy(inputs=None, outputs=[topk], comparator="Rating", ASC=False)
    gb = GroupBy(inputs=None, outputs=[orderby], key="MID", value="Rating", agg_gun="AVG")
    proj = Project(inputs=None, outputs=[gb], fields_to_keep=["MID", "Rating"])
    join = Join(left_inputs=None, right_inputs=None, outputs=[proj], left_join_attribute="UID2",
                right_join_attribute="UID")
    se1 = Select(inputs=None, predicate={"UID1": uid}, outputs=[join])
    sf = Scan(filepath=pathf, outputs=[se1])
    se2 = Select(inputs=None, predicate=None, outputs=[join])
    sr = Scan(filepath=pathr, isleft=False, outputs=[se2])
    sf.start()
    sr.start()
    assert sink.output is not None
    pass

def test_query3_pull():
    sf = Scan(filepath=pathf, outputs=None)
    sr = Scan(filepath=pathr, outputs=None)
    se1 = Select(inputs=[sf], predicate={"UID1": uid}, outputs=None)
    se2 = Select(inputs=[sr], predicate={"MID": mid}, outputs=None)
    join = Join(left_inputs=[se1], right_inputs=[se2], outputs=None, left_join_attribute="UID2",
                right_join_attribute="UID")
    proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"])
    hist = Histogram(inputs=[proj], outputs=None)
    sink = Sink(inputs=[hist], outputs=None, filepath=resPath)
    sink.get_next()
    assert sink.output is not None
    pass

def test_query3_push():
    sink = Sink(inputs=None, outputs=None, filepath=resPath)
    hist = Histogram(inputs=None, outputs=[sink])
    proj = Project(inputs=None, outputs=[hist], fields_to_keep=["Rating"])
    join = Join(left_inputs=None, right_inputs=None, outputs=[proj], left_join_attribute="UID2",
                right_join_attribute="UID")
    se1 = Select(inputs=None, predicate={"UID1": uid}, outputs=[join])
    sf = Scan(filepath=pathf, outputs=[se1])
    se2 = Select(inputs=None, predicate={"MID": mid},outputs=[join])
    sr = Scan(filepath=pathr, isleft=False, outputs=[se2])
    sf.start()
    sr.start()
    assert sink.output is not None
    pass

# def test_scan_pull():
#     sf = Scan(filepath=pathf, outputs=None)
#     a = sf.get_next()
#     assert sink.output is not None
#     pass

