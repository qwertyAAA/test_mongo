db.testcol.drop()
for (i = 1; i <= 10000; i++) {
    let arr = [];
    for (j = 1; j <= 1000; j++) {
        // key: _id, c 冗余
        // 创建索引
        // db.testcol.createIndex({"b": 1, "a": 1})
        doc = {_id: (1000 * (i - 1) + j), a: i, b: j, c: (1000 * (i - 1) + j)};
        arr.push(doc);
    }
    db.testcol.insertMany(arr);
    var x = db.testcol.find({b: 255});
    x.next();
    var x = db.testcol.find({_id: 1000 * (i - 1) + 255});
    x.next();
    var x = "asdf";
    db.testcol.updateOne({a: i, b: 255}, {$set: {d: x.pad(1000)}});
    print(i)
}