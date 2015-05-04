import difflib
import pymysql
import re
import urllib

config = {
    'replace_table': False,
}

connection = pymysql.connect(host='localhost',
                             user='root',
                             database='developer_mozilla_org')

try:
    def tally_diffs(diffs): 
        count = 0
        for diff in diffs:
            if (diff[0] == "-" or diff[0] == "+"):
                count += 1
        return count

    def save_diffs(diffs):
        if (config['replace_table']):
            try: 
                sql = """
                      INSERT INTO diff_counts VALUES
                      (%s, %s, %s) 
                      """ % (diffs['revision_id'], diffs['overall_diffs'], diffs['compat_diffs'])

                cursor.execute(sql)
                connection.commit()

            except pymysql.err.IntegrityError:
                print "error on: %s" % diffs
                pass

    def find_diffs(child_id, parent_content, child_content):
        diffs = {
            'revision_id': child_id,
            'overall_diffs': 0,
            'compat_diffs': 0
        }

        diffs['overall_diffs'] = tally_diffs(difflib.ndiff(parent_content.splitlines(), child_content.splitlines()))

        p = re.compile('(CompatibilityTable.*?)(<h2 id="See_also".+)?$', re.DOTALL)
        if p.search(child_content, re.DOTALL):
            child_compat = p.findall(child_content)[0][0]
            if p.search(parent_content, re.DOTALL):
                parent_compat = p.findall(parent_content)[0][0]
                diffs['compat_diffs'] = tally_diffs(difflib.ndiff(parent_compat.splitlines(), child_compat.splitlines()))

        return diffs

    def compare_with_parent(parent_id, child_id, child_content):
        sql = """
              SELECT content, based_on_id
              FROM wiki_revision 
              WHERE id = %s
              AND YEAR(created) >= YEAR(CURRENT_DATE - INTERVAL 1 YEAR)
              AND based_on_id IS NOT NULL
              """ % parent_id

        cursor.execute(sql)

        if (cursor.rowcount > 0):
            parent_revision = cursor.fetchone()

            print "comparing %s to %s" % (parent_id, child_id)
            diffs = find_diffs(child_id, parent_revision[0], child_content)
            save_diffs(diffs)

            # move parent into child and recurse
            # this is verbose on purpose
            new_parent_id = parent_revision[1]
            new_child_id = parent_id
            new_child_content = parent_revision[0]
            compare_with_parent(new_parent_id, new_child_id, new_child_content)

    with connection.cursor() as cursor:
        if (config['replace_table']):
            cursor.execute("DROP TABLE IF EXISTS diff_counts")
            cursor.execute("""
                           CREATE TABLE diff_counts (revision_id INT, diffs INT, compat_diffs INT,
                           PRIMARY KEY(revision_id))
                           """)
        

        sql = """
              SELECT id, content, based_on_id
              FROM wiki_revision
              WHERE id IN
              (SELECT MAX(id) AS last_rev
              FROM wiki_revision 
              WHERE YEAR(created) >= YEAR(CURRENT_DATE - INTERVAL 1 YEAR) 
              GROUP BY document_id) 
              AND based_on_id IS NOT NULL
              """

        cursor.execute(sql)
        seed_revisions = cursor.fetchall()

        for revision in seed_revisions:
            print ("processing %s" % revision[0])
            compare_with_parent(revision[2], revision[0], revision[1])

finally:
    connection.close()
