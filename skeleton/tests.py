import pytest
from assignment_12 import Scan, Select, Join, ATuple, Project, GroupBy, Histogram, TopK, OrderBy, Sink

pathf = "../data/toyf.txt"
pathr = "../data/toym.txt"
uid = 10
mid = 3
resPath = "../data/res.txt"


def test_push_scan():
    sink = Sink(inputs=None, outputs=None, track_prov=True, pull=False, filepath=resPath)
    sr = Scan(filepath=pathf, isleft=False, track_prov=True, outputs=[sink])
    sr.start()
    answer = [('1', '1'), ('2', '4'), ('3', '2')]
    temp = sink.output[1]
    lineage = temp[0].lineage()
    assert lineage == ('1', '1')
    pass


def test_push_select():
    sink = Sink(inputs=None, outputs=None, track_prov=True, filepath=resPath)
    se1 = Select(inputs=None, track_prov=True, predicate={"UID1": 1}, outputs=[sink])
    sr = Scan(filepath=pathf, track_prov=True, isleft=False, outputs=[se1])
    sr.start()
    answer = [('1', '1'), ]
    temp = sink.output[1]
    lineage = temp[0].lineage()
    assert lineage == answer[0]
    pass


def test_push_join():
    sink = Sink(inputs=None, outputs=None, track_prov=True, filepath=resPath)
    join = Join(left_inputs=None, right_inputs=None, track_prov=True, outputs=[sink], left_join_attribute="UID2",
                right_join_attribute="UID")
    se1 = Select(inputs=None, predicate={"UID1": 1}, track_prov=True, outputs=[join])
    sf = Scan(filepath=pathf, track_prov=True, outputs=[se1])
    se2 = Select(inputs=None, predicate={"MID": 1}, track_prov=True, outputs=[join])
    sr = Scan(filepath=pathr, isleft=False, track_prov=True, outputs=[se2])
    sf.start()
    sr.start()
    answer = [('1', '1'), ('1', '1', '1'), ]
    temp = sink.output[1]
    lineage = temp[0].lineage()
    assert lineage == answer
    pass


def test_push_project():
    sink = Sink(inputs=None, track_prov=True, outputs=None, filepath=resPath)
    proj = Project(inputs=None, track_prov=True, outputs=[sink], fields_to_keep=["Rating"])
    join = Join(left_inputs=None, track_prov=True, right_inputs=None, outputs=[proj], left_join_attribute="UID2",
                right_join_attribute="UID")
    se1 = Select(inputs=None, track_prov=True, predicate={"UID1": 1}, outputs=[join])
    sf = Scan(filepath=pathf, track_prov=True, outputs=[se1])
    se2 = Select(inputs=None, track_prov=True, predicate={"MID": 1}, outputs=[join])
    sr = Scan(filepath=pathr, track_prov=True, isleft=False, outputs=[se2])
    sf.start()
    sr.start()

    answer = [('1', '1'), ('1', '1', '1'), ]
    temp = sink.output[1]
    lineage = temp[0].lineage()
    assert lineage == answer

    pass


def test_push_groupby():
    sink = Sink(inputs=None, track_prov=True, outputs=None, filepath=resPath)
    groupby = GroupBy(inputs=None, track_prov=True, outputs=[sink], key="", value="Rating", agg_gun="AVG")
    proj = Project(inputs=None, track_prov=True, outputs=[groupby], fields_to_keep=["Rating"])
    join = Join(left_inputs=None, track_prov=True, right_inputs=None, outputs=[proj], left_join_attribute="UID2",
                right_join_attribute="UID")
    se1 = Select(inputs=None, track_prov=True, predicate={"UID1": 1}, outputs=[join])
    sf = Scan(filepath=pathf, track_prov=True, outputs=[se1])
    se2 = Select(inputs=None, track_prov=True, predicate={"MID": 1}, outputs=[join])
    sr = Scan(filepath=pathr, track_prov=True, isleft=False, outputs=[se2])
    sf.start()
    sr.start()

    answer = [('1', '1'), ('1', '1', '1'), ]
    temp = sink.output[1]
    lineage = temp[0].lineage()
    assert lineage == answer
    pass


def test_push_orderby():
    sink = Sink(inputs=None, track_prov=True, outputs=None, filepath=resPath)
    orderby = OrderBy(inputs=None, outputs=[sink], track_prov=True, comparator="Rating", ASC=False)
    proj = Project(inputs=None, outputs=[orderby], track_prov=True, fields_to_keep=["Rating"])
    join = Join(left_inputs=None, right_inputs=None, outputs=[proj], track_prov=True, left_join_attribute="UID2",
                right_join_attribute="UID")
    se1 = Select(inputs=None, predicate={"UID1": 1}, track_prov=True, outputs=[join])
    sf = Scan(filepath=pathf, track_prov=True, outputs=[se1])
    se2 = Select(inputs=None, predicate={"MID": 1}, track_prov=True, outputs=[join])
    sr = Scan(filepath=pathr, isleft=False, track_prov=True, outputs=[se2])
    sf.start()
    sr.start()
    answer = [('1', '1'), ('1', '1', '1'), ]
    temp = sink.output[1]
    lineage = temp[0].lineage()
    assert lineage == answer
    pass


#
def test_push_topk():
    sink = Sink(inputs=None, track_prov=True, outputs=None, filepath=resPath)
    topk = TopK(inputs=None, track_prov=True, outputs=[sink], k=1)
    orderby = OrderBy(inputs=None, track_prov=True, outputs=[topk], comparator="Rating", ASC=False)
    proj = Project(inputs=None, track_prov=True, outputs=[orderby], fields_to_keep=["Rating"])
    join = Join(left_inputs=None, track_prov=True, right_inputs=None, outputs=[proj], left_join_attribute="UID2",
                right_join_attribute="UID")
    se1 = Select(inputs=None, track_prov=True, predicate={"UID1": 1}, outputs=[join])
    sf = Scan(filepath=pathf, track_prov=True, outputs=[se1])
    se2 = Select(inputs=None, track_prov=True, predicate={"MID": 1}, outputs=[join])
    sr = Scan(filepath=pathr, track_prov=True, isleft=False, outputs=[se2])
    sf.start()
    sr.start()
    answer = [('1', '1'), ('1', '1', '1'), ]
    temp = sink.output[1]
    lineage = temp[0].lineage()
    assert lineage == answer
    pass


def test_push_hist():
    sink = Sink(inputs=None, track_prov=True, outputs=None, filepath=resPath)
    hist = Histogram(inputs=None, track_prov=True, outputs=[sink])
    topk = TopK(inputs=None, outputs=[hist], track_prov=True, k=1)
    proj = Project(inputs=None, outputs=[topk], track_prov=True, fields_to_keep=["Rating"])
    join = Join(left_inputs=None, right_inputs=None, outputs=[proj], track_prov=True, left_join_attribute="UID2",
                right_join_attribute="UID")
    se1 = Select(inputs=None, track_prov=True, predicate={"UID1": 1}, outputs=[join])
    sf = Scan(filepath=pathf, track_prov=True, outputs=[se1])
    se2 = Select(inputs=None, predicate={"MID": 1}, track_prov=True, outputs=[join])
    sr = Scan(filepath=pathr, isleft=False, track_prov=True, outputs=[se2])
    sf.start()
    sr.start()
    answer = [('1', '1'), ('1', '1', '1'), ]
    temp = sink.output[1]
    lineage = temp[0].lineage()
    assert lineage == answer
    pass

# ——————————


def test_pull_scan():
    sf = Scan(filepath=pathf, track_prov=True, outputs=None)
    answer = [('1', '1'), ('2', '4'), ('3', '2')]
    temp = sf.get_next()[1]
    lineage = temp[0].lineage()
    assert lineage == answer[0]
    pass


def test_pull_select():
    sf = Scan(filepath=pathf, track_prov=True, outputs=None)
    se = Select(inputs=[sf], track_prov=True, predicate={"UID1": 1}, outputs=None)
    temp = se.get_next()[1]
    answer = [('1', '1')]
    lineage = temp[0].lineage()
    assert lineage == answer[0]
    pass


# def LINEAGE():
#     sink = Sink(inputs=None, outputs=None, track_prov=True,filepath=resPath)
#     se1 = Select(inputs=None, predicate={"UID1": 1}, track_prov=True,outputs=[sink])
#     sr = Scan(filepath=pathf, isleft=False,track_prov=True, outputs=[se1])
#     sr.start()
#
#     temp = sink.[1]
#     answer = [('1', '1')]
#     temp[0].lineage()
#     assert temp[0].tuple == answer[0]
#     pass
#
#
# LINEAGE()


def test_pull_join():
    sf = Scan(filepath=pathf, outputs=None, track_prov=True)
    se = Select(inputs=[sf], predicate={"UID1": 1}, outputs=None, track_prov=True)
    sr = Scan(filepath=pathr, outputs=None, track_prov=True)
    se1 = Select(inputs=[sr], predicate={"MID": 1}, outputs=None, track_prov=True)
    join = Join(left_inputs=[se], right_inputs=[se1], outputs=None, left_join_attribute="UID2",
                right_join_attribute="UID", track_prov=True)

    answer = [('1', '1'), ('1', '1', '1')]
    t = join.get_next()[1]
    l = t[0].lineage()
    assert l == answer
    pass


def test_pull_project():
    sf = Scan(filepath=pathf, outputs=None, track_prov=True)
    se = Select(inputs=[sf], predicate={"UID1": 1}, track_prov=True, outputs=None)
    sr = Scan(filepath=pathr, outputs=None, track_prov=True)
    se1 = Select(inputs=[sr], predicate={"MID": 1}, outputs=None, track_prov=True)
    join = Join(left_inputs=[se], right_inputs=[se1], outputs=None, track_prov=True, left_join_attribute="UID2",
                right_join_attribute="UID")
    proj = Project(inputs=[join], outputs=None, track_prov=True, fields_to_keep=["Rating"])

    answer = [('1', '1'), ('1', '1', '1')]
    t = proj.get_next()[1]
    lineage = t[0].lineage()
    assert lineage == answer
    pass


def test_pull_groupby():
    sf = Scan(filepath=pathf, track_prov=True, outputs=None)
    se = Select(inputs=[sf], track_prov=True, predicate={"UID1": 1}, outputs=None)
    sr = Scan(filepath=pathr, track_prov=True, outputs=None)
    se1 = Select(inputs=[sr], track_prov=True, predicate={"MID": 1}, outputs=None)
    join = Join(left_inputs=[se], track_prov=True, right_inputs=[se1], outputs=None, left_join_attribute="UID2",
                right_join_attribute="UID")
    proj = Project(inputs=[join], track_prov=True, outputs=None, fields_to_keep=["Rating"])
    groupby = GroupBy(inputs=[proj], track_prov=True, outputs=None, key="", value="Rating", agg_gun="AVG")
    answer = [('1', '1'), ('1', '1', '1')]
    t = groupby.get_next()[1]
    lineage = t[0].lineage()
    assert lineage == answer
    pass


def test_pull_orderby():
    sf = Scan(filepath=pathf, track_prov=True, outputs=None)
    se = Select(inputs=[sf], predicate={"UID1": 1}, track_prov=True, outputs=None)
    sr = Scan(filepath=pathr, track_prov=True, outputs=None)
    se1 = Select(inputs=[sr], predicate={"MID": 1}, track_prov=True, outputs=None)
    join = Join(left_inputs=[se], right_inputs=[se1], outputs=None, track_prov=True, left_join_attribute="UID2",
                right_join_attribute="UID")
    proj = Project(inputs=[join], outputs=None, track_prov=True, fields_to_keep=["Rating"])
    groupby = GroupBy(inputs=[proj], outputs=None, key="", value="Rating", track_prov=True, agg_gun="AVG")
    orderby = OrderBy(inputs=[groupby], outputs=None, comparator="Rating", track_prov=True, ASC=False)
    answer = [('1', '1'), ('1', '1', '1')]
    t = orderby.get_next()[1]
    lineage = t[0].lineage()
    assert lineage == answer
    pass


def test_pull_topk():
    sf = Scan(filepath=pathf, track_prov=True, outputs=None)
    se = Select(inputs=[sf], predicate={"UID1": 1}, track_prov=True, outputs=None)
    sr = Scan(filepath=pathr, track_prov=True, outputs=None)
    se1 = Select(inputs=[sr], predicate={"MID": 1}, track_prov=True, outputs=None)
    join = Join(left_inputs=[se], right_inputs=[se1], outputs=None, track_prov=True, left_join_attribute="UID2",
                right_join_attribute="UID")
    proj = Project(inputs=[join], outputs=None, track_prov=True, fields_to_keep=["Rating"])
    groupby = GroupBy(inputs=[proj], outputs=None, key="", track_prov=True, value="Rating", agg_gun="AVG")
    orderby = OrderBy(inputs=[groupby], outputs=None, comparator="Rating", track_prov=True, ASC=False)
    topk = TopK(inputs=[orderby], outputs=None, track_prov=True, k=1)

    answer = [('1', '1'), ('1', '1', '1')]
    t = topk.get_next()[1]
    lineage = t[0].lineage()
    assert lineage == answer
    pass


def test_pull_hist():
    sf = Scan(filepath=pathf, outputs=None, track_prov=True)
    se = Select(inputs=[sf], predicate={"UID1": 1}, track_prov=True, outputs=None)
    sr = Scan(filepath=pathr, outputs=None, track_prov=True)
    se1 = Select(inputs=[sr], predicate={"MID": 1}, outputs=None, track_prov=True)
    join = Join(left_inputs=[se], right_inputs=[se1], outputs=None, left_join_attribute="UID2",
                right_join_attribute="UID", track_prov=True)
    proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"], track_prov=True)
    groupby = GroupBy(inputs=[proj], outputs=None, key="", value="Rating", agg_gun="AVG", track_prov=True)
    orderby = OrderBy(inputs=[groupby], outputs=None, comparator="Rating", ASC=False, track_prov=True)
    hist = Histogram(inputs=[orderby], outputs=None, track_prov=True)
    answer = [('1', '1'), ('1', '1', '1')]
    t = hist.get_next()[1]
    lineage = t[0].lineage()
    assert lineage == answer
    pass


def test_sink():
    sink = Sink(inputs=None, outputs=None, filepath=resPath)
    topk = TopK(inputs=None, outputs=[sink], k=1)
    orderby = OrderBy(inputs=None, outputs=[topk], comparator="Rating", ASC=False)
    proj = Project(inputs=None, outputs=[orderby], fields_to_keep=["Rating"])
    join = Join(left_inputs=None, right_inputs=None, outputs=[proj], left_join_attribute="UID2",
                right_join_attribute="UID")
    se1 = Select(inputs=None, predicate={"UID1": 1}, outputs=[join])
    sf = Scan(filepath=pathf, outputs=[se1])
    se2 = Select(inputs=None, predicate={"MID": 1}, outputs=[join])
    sr = Scan(filepath=pathr, isleft=False, outputs=[se2])
    sf.start()
    sr.start()
    answer = [['1', ], ]
    t = sink.output[1]
    for i in range(len(answer)):
        assert t[i].tuple == answer[i]
    pass
