import sqlite3
from datetime import datetime as dt

# Start
t1 = dt.now()
print('Started at: ' + str(t1))

db = sqlite3.connect(r'C:\SQLite\bol.db')
cursor = db.cursor()

# Prepare database
table_list = ['00', '10', '20', '30', '40', '50', '60', '61', '62', '70', '71', '80']
for t in table_list:
    sql = 'DELETE FROM type' + t
    cursor.execute(sql)
db.commit()

# Local file to process
f = open(r'C:\Users\andy.noble\Desktop\BOL Data\ACEFOI.20170102.txt')

line_count: int = 0
# Genscape ID
#   Each BOL set starts with an ID row and subsequent rows need to be linked
#   Only update this ID at each ID row
genid = 0


for line in f:
    # Get record type
    rec_type = line[0:2]

    # BOL Identification
    if rec_type == '00':
        genid += 1
        separators = [2, 52, 102, 152, 202, 204, 210, 218, 226, 400]
    # BOL Header
    elif rec_type == '10':
        separators = (
            2, 6, 8, 33, 37, 45, 46, 62, 73, 78, 89, 92, 103, 106, 107, 157, 161, 162, 178, 180, 205, 207, 209, 213,
            217, 221, 225, 229, 233, 237, 241, 245, 249, 257, 400)
    # Container/Equipment
    elif rec_type == '20':
        separators = (
            2, 19, 34, 49, 51, 56, 64, 72, 76, 77, 79, 96, 111, 126, 128, 133, 141, 149, 153, 154, 156, 173, 188, 203,
            205, 210, 218, 226, 230, 231, 233, 250, 265, 280, 282, 287, 295, 303, 307, 308, 310, 327, 342, 357, 359,
            364, 372, 380, 384, 385, 387, 400)
    # Shipper
    elif rec_type == '30':
        separators = (2, 37, 87, 137, 187, 237, 287, 289, 299, 301, 325, 327, 391, 400)
    # Consignee
    elif rec_type == '40':
        separators = (2, 37, 87, 137, 187, 237, 287, 289, 299, 301, 325, 327, 391, 400)
    # Notidy Party
    elif rec_type == '50':
        separators = (2, 37, 87, 137, 187, 237, 287, 289, 299, 301, 325, 327, 391, 400)
    # Cargo Description
    elif rec_type == '60':
        separators = (2, 19, 22, 32, 77, 122, 167, 212, 257, 302, 347, 392, 400)
    # Cargo Description Tariff Information
    elif rec_type == '61':
        separators = (
            2, 19, 22, 32, 40, 50, 53, 64, 74, 82, 92, 95, 106, 116, 124, 134, 137, 148, 158, 166, 176, 179, 190, 200,
            208, 218, 221, 232, 242, 250, 260, 263, 274, 284, 292, 302, 305, 316, 326, 334, 344, 347, 358, 368, 376,
            386, 389, 400)
    # Additional Cargo Description Text
    elif rec_type == '62':
        separators = (2, 19, 22, 67, 112, 157, 202, 247, 292, 337, 382, 400)
    # HAZMAT
    elif rec_type == '70':
        separators = (2, 19, 22, 32, 36, 37, 61, 67, 70, 71, 74, 330, 400)
    # HAZMAT Classification
    elif rec_type == '71':
        separators = (2, 19, 22, 278, 400)
    # Marks/Numbers
    elif rec_type == '80':
        separators = (2, 19, 64, 109, 154, 199, 244, 289, 334, 379, 400)

    start_sep = 0
    i = 0
    value_list = [genid]
    for sep in separators:
        value_list.append(line[start_sep:sep])
        i += 1
        start_sep = sep

    var_string = ', '.join('?' * len(value_list))
    sql = 'INSERT INTO type' + rec_type + ' VALUES (%s)' % var_string
    cursor.execute(sql, value_list)
    line_count += 1
    if line_count % 5000 == 0:
        print(line_count)

    # print(genid)
    # for e in range(i):
    #    print('Col '+str(e)+': '+str(value_list[e]))

db.close

# Finish
# Commit at the end
db.commit()
t2 = dt.now()
print('Finished at: ' + str(t2))
diff = t2 - t1
print('Duration in seconds: ' + str(diff))
