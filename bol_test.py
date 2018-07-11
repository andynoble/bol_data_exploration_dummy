import sqlite3
import datetime

# Local file to process
f = open(r"C:\Users\andy.noble\Desktop\BOL Data\ACEFOI.20170102.txt")
trunc_data = 'Y'

# Purely for output to give some sense that something is happening
line_count = 0

# Create database connection and cursor
db = sqlite3.connect(r"C:\SQLite\bol.db")
cursor = db.cursor()

# Define the column widths for different record types in the file
def get_boltype (in_rectype):
    global genscapeid
    # BOL Identification
    if in_rectype == '00':
        genscapeid += 1 # Only increment if this is an ID record type
        out_separators = [2, 52, 102, 152, 202, 204, 210, 218, 226, 400]
    # BOL Header
    elif in_rectype == '10':
        out_separators = (
            2, 6, 8, 33, 37, 45, 46, 62, 73, 78, 89, 92, 103, 106, 107, 157, 161, 162, 178, 180, 205, 207, 209, 213,
            217, 221, 225, 229, 233, 237, 241, 245, 249, 257, 400)
    # Container/Equipment
    elif in_rectype == '20':
        out_separators = (
            2, 19, 34, 49, 51, 56, 64, 72, 76, 77, 79, 96, 111, 126, 128, 133, 141, 149, 153, 154, 156, 173, 188, 203,
            205, 210, 218, 226, 230, 231, 233, 250, 265, 280, 282, 287, 295, 303, 307, 308, 310, 327, 342, 357, 359,
            364, 372, 380, 384, 385, 387, 400)
    # Shipper
    elif in_rectype == '30':
        out_separators = (2, 37, 87, 137, 187, 237, 287, 289, 299, 301, 325, 327, 391, 400)
    # Consignee
    elif in_rectype == '40':
        out_separators = (2, 37, 87, 137, 187, 237, 287, 289, 299, 301, 325, 327, 391, 400)
    # Notidy Party
    elif in_rectype == '50':
        out_separators = (2, 37, 87, 137, 187, 237, 287, 289, 299, 301, 325, 327, 391, 400)
    # Cargo Description
    elif in_rectype == '60':
        out_separators = (2, 19, 22, 32, 77, 122, 167, 212, 257, 302, 347, 392, 400)
    # Cargo Description Tariff Information
    elif in_rectype == '61':
        out_separators = (
            2, 19, 22, 32, 40, 50, 53, 64, 74, 82, 92, 95, 106, 116, 124, 134, 137, 148, 158, 166, 176, 179, 190, 200,
            208, 218, 221, 232, 242, 250, 260, 263, 274, 284, 292, 302, 305, 316, 326, 334, 344, 347, 358, 368, 376,
            386, 389, 400)
    # Additional Cargo Description Text
    elif in_rectype == '62':
        out_separators = (2, 19, 22, 67, 112, 157, 202, 247, 292, 337, 382, 400)
    # HAZMAT
    elif in_rectype == '70':
        out_separators = (2, 19, 22, 32, 36, 37, 61, 67, 70, 71, 74, 330, 400)
    # HAZMAT Classification
    elif in_rectype == '71':
        out_separators = (2, 19, 22, 278, 400)
    # Marks/Numbers
    elif in_rectype == '80':
        out_separators = (2, 19, 64, 109, 154, 199, 244, 289, 334, 379, 400)
    else:
        out_separators = None

    return out_separators

# Start
t1 = datetime.datetime.now()
print('Started at: '+str(t1))

# Prepare database
if trunc_data == 'Y':
    table_list = ['00', '10', '20', '30', '40', '50', '60', '61', '62', '70', '71', '80']
    for t in table_list:
        sql = 'DELETE FROM type' + t
        cursor.execute(sql)
    db.commit()
    # Genscape ID
    #   Each BOL set starts with an ID row and subsequent rows need to be linked
    #   Only update this ID at each Record ID row in the source data
    #   If truncating the tables, set this to 0
    genscapeid = 0
else:
    #   If not truncating the tables, get the latest value from the DB
    sql = 'SELECT max(gen_id) FROM type00'
    cursor.execute(sql)
    genscapeid = cursor.fetchone()[0]

for line in f:
    # Get record type
    rectype = line[0:2]
    separators = get_boltype(rectype)

    start_sep = 0
    i = 0
    value_list = [genscapeid]
    for sep in separators:
        value_list.append(line[start_sep:sep].strip())
        i += 1
        start_sep = sep

    var_string = ', '.join('?' * len(value_list))
    sql = 'INSERT INTO type' + rectype + ' VALUES (%s)' % var_string
    try:
        cursor.execute(sql, value_list)
    except sqlite3.IntegrityError as e:
        auditsql = 'INSERT INTO type00_deletes SELECT * FROM type00 WHERE col2 = ? AND col3 = ?'
        delsql = 'DELETE FROM type00 WHERE col2 = ? AND col3 = ?'
        delvalue_list = value_list[2:4]
        cursor.execute(auditsql, delvalue_list)
        cursor.execute(delsql, delvalue_list)
        cursor.execute(sql, value_list)
    line_count += 1
    if line_count % 5000 == 0:
        print(line_count)

# Finish
# Commit at the end
db.commit()
db.close()

# How long did it take to run
t2 = datetime.datetime.now()
print('Finished at: '+str(t2))
diff = t2 - t1
print('Duration in seconds: '+(str(diff)))
print('To process: '+str(line_count)+' records')