// get data
val all_files = sc.wholeTextFiles("project/toronto_data")
val inp = all_files.map(x=>x._2).flatMap(x=>x.split("\\n")).filter(x=>x!=null && x.length>=61)

val inp2 = inp.map(x=>{if (x.charAt(x.length()-1)==',') x.concat("0") else x})
val inp3 = inp2.map(x=>x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))

// remove in case split not done correctly
val inp4 = inp3.filter(x=>x!=null && x.length==62)

// get relevant columns
val inp5 = inp4.map(y=>Array(y(0),y(1),y(2),y(4),y(8),y(9),y(10),y(14),y(15),y(16),y(18),y(19),y(20),y(21),y(22),y(23),y(24),y(25),y(27),y(28),y(29),y(30),y(31),y(43),y(46),y(47),y(48),y(49),y(50),y(51),y(52),y(57),y(61)))


// remove the title row
val header = inp5.first
val data_without_header = inp5.filter(x=>x(0)!="id")

// remove entries where price is not given
val data = data_without_header.filter(x=>x(18)!=null && x(18)!="N/A" && x(18)!="")

