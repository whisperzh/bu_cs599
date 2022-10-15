import pytest
from assignment_12 import Scan,Select,Join,ATuple,Project,GroupBy,Histogram,TopK,OrderBy

pathf = "../data/toyf.txt"
pathr = "../data/toym.txt"
uid = 10
mid = 3
resPath = "../data/res.txt"

#
# def test_query1_pull():
#     sf = Scan(filepath=pathf, outputs=None)
#     sr = Scan(filepath=pathr, outputs=None)
#     se1 = Select(inputs=[sf], predicate={"UID1": uid}, outputs=None)
#     se2 = Select(inputs=[sr], predicate={"MID": mid}, outputs=None)
#     join = Join(left_inputs=[se1], right_inputs=[se2], outputs=None, left_join_attribute="UID2",
#                 right_join_attribute="UID")
#     proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"])
#     groupby = GroupBy(inputs=[proj], outputs=None, key="", value="Rating", agg_gun="AVG")
#     sink = Sink(inputs=[groupby], outputs=None, filepath=resPath)
#     sink.get_next()
#     assert sink.output is not None
#     pass
#
#
# def test_query1_push():
#     sink = Sink(inputs=None, outputs=None, filepath=resPath)
#     groupby = GroupBy(inputs=None, outputs=[sink], key="", value="Rating", agg_gun="AVG")
#     proj = Project(inputs=None, outputs=[groupby], fields_to_keep=["Rating"])
#     join = Join(left_inputs=None, right_inputs=None, outputs=[proj], left_join_attribute="UID2",
#                 right_join_attribute="UID")
#     se1 = Select(inputs=None, predicate={"UID1": uid}, outputs=[join])
#     sf = Scan(filepath=pathf, outputs=[se1])
#     se2 = Select(inputs=None, predicate={"MID": mid}, outputs=[join])
#     sr = Scan(filepath=pathr, isleft=False, outputs=[se2])
#     sf.start()
#     sr.start()
#     assert sink.output is not None
#     pass
#
#
# def test_query2_pull():
#     sf = Scan(filepath=pathf, outputs=None)
#     sr = Scan(filepath=pathr, outputs=None)
#     se1 = Select(inputs=[sf], predicate={"UID1": uid}, outputs=None)
#     se2 = Select(inputs=[sr], predicate=None, outputs=None)
#     join = Join(left_inputs=[se1], right_inputs=[se2], outputs=None, left_join_attribute="UID2",
#                 right_join_attribute="UID")
#     proj = Project(inputs=[join], outputs=None, fields_to_keep=["MID", "Rating"])
#     groupby = GroupBy(inputs=[proj], outputs=None, key="MID", value="Rating", agg_gun="AVG")
#     orderby = OrderBy(inputs=[groupby], outputs=None, comparator="Rating", ASC=False)
#     topk = TopK(inputs=[orderby], outputs=None, k=1)
#     sink = Sink(inputs=[topk], outputs=None, filepath=resPath)
#     sink.get_next()
#     assert sink.output is not None
#     pass
#
#
# def test_query2_push():
#     sink = Sink(inputs=None, outputs=None, filepath=resPath)
#     topk = TopK(inputs=None, outputs=[sink], k=1)
#     orderby = OrderBy(inputs=None, outputs=[topk], comparator="Rating", ASC=False)
#     gb = GroupBy(inputs=None, outputs=[orderby], key="MID", value="Rating", agg_gun="AVG")
#     proj = Project(inputs=None, outputs=[gb], fields_to_keep=["MID", "Rating"])
#     join = Join(left_inputs=None, right_inputs=None, outputs=[proj], left_join_attribute="UID2",
#                 right_join_attribute="UID")
#     se1 = Select(inputs=None, predicate={"UID1": uid}, outputs=[join])
#     sf = Scan(filepath=pathf, outputs=[se1])
#     se2 = Select(inputs=None, predicate=None, outputs=[join])
#     sr = Scan(filepath=pathr, isleft=False, outputs=[se2])
#     sf.start()
#     sr.start()
#     assert sink.output is not None
#     pass
#
#
# def test_query3_pull():
#     sf = Scan(filepath=pathf, outputs=None)
#     sr = Scan(filepath=pathr, outputs=None)
#     se1 = Select(inputs=[sf], predicate={"UID1": uid}, outputs=None)
#     se2 = Select(inputs=[sr], predicate={"MID": mid}, outputs=None)
#     join = Join(left_inputs=[se1], right_inputs=[se2], outputs=None, left_join_attribute="UID2",
#                 right_join_attribute="UID")
#     proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"])
#     hist = Histogram(inputs=[proj], outputs=None)
#     sink = Sink(inputs=[hist], outputs=None, filepath=resPath)
#     sink.get_next()
#     assert sink.output is not None
#     pass
#
#
# def test_query3_push():
#     sink = Sink(inputs=None, outputs=None, filepath=resPath)
#     hist = Histogram(inputs=None, outputs=[sink])
#     proj = Project(inputs=None, outputs=[hist], fields_to_keep=["Rating"])
#     join = Join(left_inputs=None, right_inputs=None, outputs=[proj], left_join_attribute="UID2",
#                 right_join_attribute="UID")
#     se1 = Select(inputs=None, predicate={"UID1": uid}, outputs=[join])
#     sf = Scan(filepath=pathf, outputs=[se1])
#     se2 = Select(inputs=None, predicate={"MID": mid}, outputs=[join])
#     sr = Scan(filepath=pathr, isleft=False, outputs=[se2])
#     sf.start()
#     sr.start()
#     assert sink.output is not None
#     pass


# sf = None
# sr = None
# se1 = Select(inputs=[sf], predicate={"UID1": uid}, outputs=None)
# se2 = Select(inputs=[sr], predicate={"MID": mid}, outputs=None)
# join = Join(left_inputs=[se1], right_inputs=[se2], outputs=None, left_join_attribute="UID2",
#             right_join_attribute="UID")
# proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"])
# groupby = GroupBy(inputs=[proj], outputs=None, key="", value="Rating", agg_gun="AVG")
# sink = Sink(inputs=[groupby], outputs=None, filepath=resPath)
# sink.get_next()


# def test_push_scan():
#     pass
#
# def test_push_select():
#     pass
#
# def test_push_join():
#     pass
#
# def test_push_project():
# pass
#
# def test_push_groupby():
# pass
#
# def test_push_orderby():
# pass
#
# def test_push_topk():
# pass
#
# def test_push_hist():
# pass
#
# #——————————
#
def test_pull_scan():
    sf = Scan(filepath=pathf, outputs=None)
    answer = [('1', '1'), ('2', '4'), ('3', '2')]
    temp = sf.get_next()[1]
    for i in range(len(answer)):
        assert temp[i].tuple == answer[i]
    pass

def test_pull_select():
    sf = Scan(filepath=pathf, outputs=None)
    se = Select(inputs=[sf], predicate={"UID1": 1}, outputs=None)
    temp = se.get_next()[1]
    answer = [('1', '1')]
    assert temp[0].tuple == answer[0]
    pass

def test_pull_join():
    sf = Scan(filepath=pathf, outputs=None)
    se = Select(inputs=[sf], predicate={"UID1": 1}, outputs=None)
    sr = Scan(filepath=pathr, outputs=None)
    se1 = Select(inputs=[sr], predicate={"MID": 1}, outputs=None)
    join = Join(left_inputs=[se], right_inputs=[se1], outputs=None, left_join_attribute="UID2",
                right_join_attribute="UID")

    answer = [('1', '1', '1', '1', '1')]
    t=join.get_next()[1]
    for i in range(len(answer)):
        assert t[i].tuple == answer[i]
    pass

def test_pull_project():
    sf = Scan(filepath=pathf, outputs=None)
    se = Select(inputs=[sf], predicate={"UID1": 1}, outputs=None)
    sr = Scan(filepath=pathr, outputs=None)
    se1 = Select(inputs=[sr], predicate={"MID": 1}, outputs=None)
    join = Join(left_inputs=[se], right_inputs=[se1], outputs=None, left_join_attribute="UID2",
                right_join_attribute="UID")
    proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"])

    answer = [tuple('1'),]
    t = proj.get_next()[1]
    for i in range(len(answer)):
        assert t[i].tuple == answer[i]
    pass


def test_pull_groupby():
    sf = Scan(filepath=pathf, outputs=None)
    se = Select(inputs=[sf], predicate={"UID1": 1}, outputs=None)
    sr = Scan(filepath=pathr, outputs=None)
    se1 = Select(inputs=[sr], predicate={"MID": 1}, outputs=None)
    join = Join(left_inputs=[se], right_inputs=[se1], outputs=None, left_join_attribute="UID2",
                right_join_attribute="UID")
    proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"])
    groupby = GroupBy(inputs=[proj], outputs=None, key="", value="Rating", agg_gun="AVG")
    answer = ['1.0',]
    t = groupby.get_next()[1]
    for i in range(len(answer)):
        assert str(t[i].tuple[0]) == answer[i]
    pass

def test_pull_orderby():
    sf = Scan(filepath=pathf, outputs=None)
    se = Select(inputs=[sf], predicate={"UID1": 1}, outputs=None)
    sr = Scan(filepath=pathr, outputs=None)
    se1 = Select(inputs=[sr], predicate={"MID": 1}, outputs=None)
    join = Join(left_inputs=[se], right_inputs=[se1], outputs=None, left_join_attribute="UID2",
                right_join_attribute="UID")
    proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"])
    groupby = GroupBy(inputs=[proj], outputs=None, key="", value="Rating", agg_gun="AVG")
    orderby = OrderBy(inputs=[groupby], outputs=None, comparator="Rating", ASC=False)
    answer = ['1.0', ]
    t = orderby.get_next()[1]
    for i in range(len(answer)):
        assert str(t[i].tuple[0]) == answer[i]
    pass
#
def test_pull_topk():
    sf = Scan(filepath=pathf, outputs=None)
    se = Select(inputs=[sf], predicate={"UID1": 1}, outputs=None)
    sr = Scan(filepath=pathr, outputs=None)
    se1 = Select(inputs=[sr], predicate={"MID": 1}, outputs=None)
    join = Join(left_inputs=[se], right_inputs=[se1], outputs=None, left_join_attribute="UID2",
                right_join_attribute="UID")
    proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"])
    groupby = GroupBy(inputs=[proj], outputs=None, key="", value="Rating", agg_gun="AVG")
    orderby = OrderBy(inputs=[groupby], outputs=None, comparator="Rating", ASC=False)
    topk = TopK(inputs=[orderby], outputs=None, k=1)
    answer = ['1.0', ]
    t = topk.get_next()[1]
    for i in range(len(answer)):
        assert str(t[i].tuple[0]) == answer[i]
    pass

def test_pull_hist():
    sf = Scan(filepath=pathf, outputs=None)
    se = Select(inputs=[sf], predicate={"UID1": 1}, outputs=None)
    sr = Scan(filepath=pathr, outputs=None)
    se1 = Select(inputs=[sr], predicate={"MID": 1}, outputs=None)
    join = Join(left_inputs=[se], right_inputs=[se1], outputs=None, left_join_attribute="UID2",
                right_join_attribute="UID")
    proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"])
    groupby = GroupBy(inputs=[proj], outputs=None, key="", value="Rating", agg_gun="AVG")
    orderby = OrderBy(inputs=[groupby], outputs=None, comparator="Rating", ASC=False)
    hist = Histogram(inputs=[orderby], outputs=None)
    answer = ['1.0', ]
    t = hist.get_next()[1]

    for i in range(len(answer)):
        assert str(t[i].tuple[0]) == answer[i]
    pass

# def test_sink():
#     pass
