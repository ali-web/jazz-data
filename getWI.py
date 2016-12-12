import os,re,sys,json,requests,xmltodict
from pprint import pprint
import MySQLdb
from datetime import datetime
import config
import re


# a = {}

# print a.get('abc', {}).get('lfkjsd')
# print "hi"
# exit()



#GLOBAL VARIABLES BEGIN
#cookie to sign into jazz.net - must update to current cookie before running script
cookies = config.cookies

#headers to download the correct type of files
headers = {'Accept': 'application/xml	', 'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:38.0) Gecko/20100101 Firefox/38.0'}


conn = MySQLdb.connect(host='127.0.0.1', port=3306, user='root', passwd='', db=config.db)
cursor = conn.cursor(MySQLdb.cursors.DictCursor)
snapshot_table = "snapshot"
comment_table = "comment"
ids_table = "w"
failed_w_table = 'failed_w'
change_set_table = 'change_set'
user_table = 'user'
iteration_table = 'iteration'


id_last_digit = sys.argv[1]


# cursor.execute("SELECT `id` from " + ids_table + " Where `type` = `type` AND id < 50000 AND `done` = 0")
cursor.execute("SELECT `id` from " + ids_table + " Where `done` = 0 AND MOD(id, " + str(config.partitions) + ") = " + id_last_digit)

workitem_ids = []


# print "about to begin ..."
for x in range(0, cursor.rowcount):
    row = cursor.fetchone()

    workitem_ids.append(int(row['id']))
    # big_description += ' ' + str(row['description'])

    #turn description to nltk text format
    # description = nltk.text.Text(str(row['description']))


# pprint (workitem_ids)
# exit()

# workitem_ids = [80168,80169]



def convert(data): 
	# xl = file(f)
	try:
 		result = xmltodict.parse(data)
		return "", result

	except Exception, e:
		# print str(e)
	 	return e, False 


def mark_WI_as_failed(idd):
	sql = "INSERT IGNORE INTO %s ( %s ) VALUES ( %s )" % (failed_w_table, 'w_id', idd)
	cursor.execute(sql)

	conn.commit()


def timestamp_converter(string):
	if string is None:
		return None

	try:
		timestamp = str(datetime.strptime(string[:-5], '%Y-%m-%dT%H:%M:%S.%f'))[:-3]
		timezone  = string[-5:]

		# return timestamp, timezone	
		return timestamp
	except:
		return string


# def timestamp_converter(string):
# 	timestamp = datetime.strptime(string[:-5], '%Y-%m-%dT%H:%M:%S.%f')
# 	timezone  = string[-5:]

# 	return timestamp


def sort_list_of_dic(list_of_dic):
	return sorted(list_of_dic, key=lambda k:k['modified_datetime'])
	# return list_of_dic.sort(key=lambda item:item['id'])	



def downloader():
	#goes through all workitem_ids in workitem_ids array
	for k, workitem_id in enumerate(workitem_ids):
		#store workitem_id and project_id in easily identifiable variables
		#workitem_id = workitem[0]
		#project_id = workitem[1]



		#navigates to the correct directory, creating history and historyXMLS folders if necessary
		# os.chdir(directory+"/"+project_id+"/workitems")
		# if not os.path.exists("history"):
		# 	os.mkdir("history")
		# os.chdir("history")
		# if not os.path.exists("historyXMLS"):
		# 	os.mkdir("historyXMLS")
		# os.chdir("historyXMLS")

		#put workitem_id into the middle of the url
		l1="https://jazz.net/" + config.ns + "/rpt/repository/workitem?fields=workitem/workItem[id="
		l2="]/(id|auditableLinks/(*/*)\
|(itemHistory/(id|type/(name)|state/(name)|comments/(creationDate|content|edited|creator/(userId))\
|subscriptions/(userId|name|emailAddress|archived)|modified|creationDate|resolutionDate|duration|timeSpent|correctedEstimate\
|severity/(name)|priority/(name)|foundIn/(name)\
|summary|description|tags|target/(id|name|archived|startDate|endDate|modified|itemType|hasDeliverable|children/(*))|category/(name)|creator/(userId|name|emailAddress)\
|owner/(userId|name|emailAddress)|resolver/(userId|name|emailAddress)\
|parent/(id)))))"
		url = l1 + str(workitem_id) + l2


		# print url
		# exit()

		#downloads xml data from the url
		response = requests.get(url, cookies=cookies, headers=headers)
		# print response
		# exit()
		data = response.content.decode("ascii", "ignore")

		#saves xml file as [workitem_id].xml into JSON-05-12-2015/[project_id]/workitems/history/historyXMLS
		# f=open( str(workitem_id) + ".xml",'w')
		# f.write(data)
		# f.close()
		# print data
		# exit()

		error, dictData = convert(data)

		# if xml was not valid to be convereted to dictionary
		if dictData == False:
			print "Failed to convert workitem with id " + str(workitem_id) + " to dictionary - enumeration number is: " + str(k)


			print str(error)
			f = open('xml.txt','w')
			f.write(data) # python will convert \n to os.linesep
			f.close() # you can omit in most cases as the destructor will call it

			# exit()

			mark_WI_as_failed(workitem_id)
			# print url
			continue

		dicted = dictData['workitem']['workItem']

		print dicted['id']

		# pprint(dicted)
		# exit()

		# for i, history in enumerate(dicted['itemHistory']):
		# 	history['modified_datetime'] = str(datetime.strptime(history['modified'][:-5], '%Y-%m-%dT%H:%M:%S.%f'))[:-3]
		# 	print history['modified_datetime']

		# exit()

		if isinstance(dicted['itemHistory'], dict):
			# exit('bye bye')
			all_itemHistories_unsorted = [dicted['itemHistory']]
		else:
			all_itemHistories_unsorted = dicted['itemHistory']


		# first sort item histories
		for idx, item in enumerate(all_itemHistories_unsorted):
			all_itemHistories_unsorted[idx]['modified_datetime'] = timestamp_converter(item['modified'])

		all_itemHistories = sort_list_of_dic(all_itemHistories_unsorted)

		# print type(all_itemHistories)
		# print type(all_itemHistories_unsorted)
		# pprint (all_itemHistories)
		# exit()


		# Then iterate through the sorted list
		for i, history in enumerate(all_itemHistories):
			i = i + 1

			# pprint(history)
			# exit()
			# print history.get('fdkj', {}).get('id', None)
			# exit()

			# if k == 2:
			# 	print history

			# To avoid error when parent is an empty dict
			if isinstance(history['parent'], dict):
				parent_w_id = history['parent'].get('id')
			else:
				parent_w_id = None

			if isinstance(history['foundIn'], dict):
				found_in = history['foundIn'].get('name')
			else:
				found_in = None

			if isinstance(history['category'], dict):
				filed_against = history['category'].get('name')
			else:
				filed_against = None

			if isinstance(history['target'], dict):
				# prepare planned_for attribute value
				planned_for = history['target'].get('id')
				iteration_identifier = history['target'].get('id')

				# pprint (history['target'])



				# Also populate iteration table
				if iteration_identifier is not None:
					cursor.execute("SELECT `parent_id` from " + iteration_table + " Where `identifier` = '" + iteration_identifier + "'")

					if cursor.rowcount == 0:

						row = {
							'type'       : history['target']['itemType'],
							'name'       : history['target']['name'],
							'identifier' : history['target']['id'],
							'start_date' : timestamp_converter(history['target']['startDate']),
							'end_date'   : timestamp_converter(history['target']['endDate']),
							'modified_date'   : timestamp_converter(history['target']['modified']),
							'has_deliverable'   : history['target']['hasDeliverable'],
							'archived'   : history['target']['archived'],
						}

						placeholders = ', '.join(['%s'] * len(row))
						columns = ', '.join(row.keys())
						sql = "INSERT IGNORE INTO %s ( %s ) VALUES ( %s )" % (iteration_table, columns, placeholders)
						cursor.execute(sql, row.values())

						conn.commit()

						parent_id = cursor.lastrowid

					else:
						parent_id = cursor.fetchone()['parent_id']

					# Now get the child iterations if any
					iteration = history['target']

					if 'children' in iteration:
						if isinstance(iteration['children'], dict):
							# exit('bye bye')
							all_children = [iteration['children']]
						else:
							all_children = iteration['children']


						for child in all_children:
							cursor.execute("SELECT id FROM " + iteration_table + " Where identifier = '" + child['id'] + "'")

							if cursor.rowcount == 0:
								row = {
									'parent_id'  : parent_id,
									'type'       : child['itemType'],
									'name'       : child['name'],
									'identifier' : child['id'],
									'start_date' : timestamp_converter(child['startDate']),
									'end_date'   : timestamp_converter(child['endDate']),
									'modified_date'   : timestamp_converter(child['modified']),
									'has_deliverable'   : child['hasDeliverable'],
									'archived'   : child['archived'],
								}

								placeholders = ', '.join(['%s'] * len(row))
								columns = ', '.join(row.keys())
								sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (iteration_table, columns, placeholders)
								cursor.execute(sql, row.values())

								conn.commit()


			else:
				planned_for = None

			# if isinstance(history['parent'], dict):
			# 	parent_id = history['parent'].get('name')
			# else:
			# 	parent_id = None
			history_severity = history.get('severity')
			if isinstance(history_severity, dict):
				severity = history_severity.get('name')
				if severity == 'Unassigned':
					severity = None
			else:
				severity = None

			history_priority = history.get('priority')
			if isinstance(history_priority, dict):
				priority = history_priority.get('name')
				if priority == 'Unassigned':
					priority = None
			else:
				priority = None


			if history['creator']['userId'] == 'unassigned':
				creator = None
			else:
				creator = history['creator']['userId']

			if history['owner']['userId'] == 'unassigned':
				owner = None
			else:
				owner = history['owner']['userId']

			if history['resolver']['userId'] == 'unassigned':
				resolver = None
			else:
				resolver = history['resolver']['userId']


			if history['timeSpent'] == '-1':
				time_spent_in_minute = None
			else:
				time_spent_in_minute = int(history['timeSpent']) / 60000

			if history['duration'] == '-1':
				estimate_in_minute = None
			else:
				estimate_in_minute = int(history['duration']) / 60000

			if history['correctedEstimate'] == '-1':
				corrected_estimate_in_minute = None
			else:
				corrected_estimate_in_minute = int(history['correctedEstimate']) / 60000

			story = {
				'w_id' : dicted['id'],
				'h_id' : i,
				'parent_w_id' : parent_w_id,
				'type' : history.get('type', {}).get('name'),

				# 'modified_datetime' : history['modified_datetime'],
				'modified_date' : timestamp_converter(history['modified_datetime']),
				'creation_date' : timestamp_converter(history['creationDate']),
				'resolution_date' : timestamp_converter(history['resolutionDate']),
				'time_spent' : history['timeSpent'],
				'estimate' : history['duration'],
				'corrected_estimate' : history['correctedEstimate'],

				'time_spent_in_minute' : time_spent_in_minute,
				'estimate_in_minute' : estimate_in_minute,
				'corrected_estimate_in_minute' : corrected_estimate_in_minute,

				'status' : history['state']['name'],
				'severity' : severity,
				'priority' : priority,
				'found_in' : found_in,
				'filed_against' : filed_against,
				'planned_for' : planned_for,

				# 'creator_name'  : history['creator']['name'],
				# 'creator_email' : history['creator']['emailAddress'],
				'creator_identifier'  : creator,
				# 'owner_name'  : history['owner']['name'],
				# 'owner_email' : history['owner']['emailAddress'],
				'owner_identifier'    : owner,
				# 'resolver_name'  : history['resolver']['name'],
				# 'resolver_email' : history['resolver']['emailAddress'],
				'resolver_identifier' : resolver,

				'summary'    : history['summary'],
				'description'    : history['description'],
				'tags'    : history['tags'],
			}

			# prepare subscirbers attribute
			subscription_list = ''
			# print "this is the whole string of subscriptions:"
			# print history['subscriptions']

			# print "len of subscription is: " + str(len(history['subscriptions']))

			# This block is necessary to convert subsriptions with single entry from OrderedDict to List
			if 'subscriptions' in history:
				if isinstance(history['subscriptions'], dict):
					# exit('bye bye')
					all_subscriptions = [history['subscriptions']]
				else:
					all_subscriptions = history['subscriptions']

				# print "type of all_subscriptions is: " + str(type(all_subscriptions))

				subscriber_count = 0

				for j, subscription in enumerate(all_subscriptions):
					# print type(subscription)
					# print "this is single subscription:"
					# print subscription

					if (j != 0):
						subscription_list = subscription_list + "\n"

					if 'userId' in subscription and isinstance(subscription['userId'], basestring):
						# print j
						# print subscription
						# print "type is " + str(type(subscription))
						subscription_list = subscription_list + subscription['userId']

						# Populate users table
						cursor.execute("SELECT `id` from " + user_table + " Where `identifier` = '" + subscription['userId'] + "'")

						if cursor.rowcount == 0:

							row = {
								'name'       : subscription['name'],
								'identifier' : subscription['userId'],
								'email'      : subscription['emailAddress'],
								'archived'   : subscription['archived'],
							}

							placeholders = ', '.join(['%s'] * len(row))
							columns = ', '.join(row.keys())
							sql = "INSERT IGNORE INTO %s ( %s ) VALUES ( %s )" % (user_table, columns, placeholders)
							cursor.execute(sql, row.values())

							conn.commit()

						subscriber_count = subscriber_count + 1

					else:
						# means subscriber does not exist
						continue



				story['subscribers'] = subscription_list
			else:
				story['subscribers'] = None
				subscriber_count = 0


			story['subscriber_count'] = subscriber_count


			# prepare comments list, so commments_count could be calculated at this point
			all_comments = []

			if 'comments' in history:
				# print type(history['comments'])

				if isinstance(history['comments'], dict):
					all_comments = [history['comments']]
				else:
					all_comments = history['comments']

				comment_count = len(all_comments)

			else:
				comment_count = 0

			story['comment_count'] = comment_count


			# insert historical records into story table
			# cursor.executemany("INSERT INTO "
			# 	+ story_table +
			# 	" (w_id, h_id, type, modified_date, creation_date, resolution_date, time_spent, severity, priority, found_in, filed_against, planned_for, creator_name, creator_email, creator_id, summary, description, tags) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
			# 	story.values())

			# cursor.execute("INSERT INTO %s (%s) VALUES(%s)" % story_table, ",".join(story.keys()), ",".join(story.values()))

			# qmarks = ', '.join('?' * len(story))
			# qry = "Insert Into " + story_table + " (%s) Values (%s)" % (qmarks, qmarks)
			# cursor.execute(qry, story.keys() + story.values())

			placeholders = ', '.join(['%s'] * len(story))
			columns = ', '.join(story.keys())
			sql = "INSERT IGNORE INTO %s ( %s ) VALUES ( %s )" % (snapshot_table, columns, placeholders)
			cursor.execute(sql, story.values())

			conn.commit()

			s_id = cursor.lastrowid

			# print "success in story snapshot insertion"


			#insert comments of story


			for comment in all_comments:

				row = {
					's_id' : s_id,
					'w_id' : dicted['id'],
					'creator_id' : comment['creator']['userId'],
					'creation_date' : timestamp_converter(comment['creationDate']),
					'content' : comment['content'],
					'edited' : comment['edited']
				}

				placeholders = ', '.join(['%s'] * len(row))
				columns = ', '.join(row.keys())
				sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (comment_table, columns, placeholders)
				cursor.execute(sql, row.values())

				conn.commit()



		# Mark WI as done
		cursor.execute("Update " + ids_table + " SET `done` = 1 WHERE id = '" + str(dicted['id']) + "'")

		conn.commit()








		#auditableLinks
		if ('auditableLinks' in dicted):
			for l, link in enumerate(dicted['auditableLinks']):

				if isinstance(link, dict):
					if link['name'] == 'com.ibm.team.filesystem.workitems.change_set':
						# print str(re.match(r"^Changes in(.*)- .*$", link['sourceRef' ]['comment']).group(1))
						change_set_parts = link['sourceRef' ]['comment'].split(' - ')
						change_set_changes_in = change_set_parts[0]
						change_set_changes_in = change_set_changes_in.replace("Changes in: ", "")

						if isinstance(change_set_parts, list) and len(change_set_parts) > 1:
							if change_set_parts[1] == '<No Comment>':
								change_set_comment = None
							else:
								change_set_comment = change_set_parts[1]
						else:
							change_set_comment = None


						change_set = {}

						change_set['user_identifier'] = link['modifiedBy']['userId']
						change_set['creation_date']   = timestamp_converter(link['modified'])
						change_set['modified_date']   = timestamp_converter(link['modifiedBy']['modified'])
						change_set['content']         = link['sourceRef' ]['comment']

						change_set['changes_in']      = change_set_changes_in
						change_set['comment']         = change_set_comment
						
						change_set['w_id']            = dicted['id']

						placeholders = ', '.join(['%s'] * len(change_set))
						columns = ', '.join(change_set.keys())
						sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (change_set_table, columns, placeholders)
						cursor.execute(sql, change_set.values())

						conn.commit()





		print "inserted k = " + str(k)

			#insert children of story
			###############################################		
			# if 'children' in history:
			# 	# print type(history['comments'])

			# 	if isinstance(history['children'], dict):
			# 		all_children = [history['children']]
			# 	else:
			# 		all_children = history['children']

			# for child in all_children:

			# 	WI = {
			# 		'w_id' : child['id'],
			# 		'parent_id' : dicted['id'],
			# 		'parent_s_id' : s_id,
			# 		'type' : child['type']['name'],

			# 		'modified_date' : child['modified'],
			# 		'creation_date' : child['creationDate'],
			# 		'resolution_date' : child['resolutionDate'],
			# 		'time_spent' : child['timeSpent'],

			# 		'severity' : child['severity'],
			# 		'priority' : child['priority'],
			# 		'found_in' : child['foundIn'],
			# 		'filed_against' : child['category']['name'],
			# 		# 'planned_for' : history['target']['id'],

			# 		'creator_name'  : child['creator']['name'],
			# 		'creator_email' : child['creator']['emailAddress'],
			# 		'creator_id'    : child['creator']['userId'],
			# 		'owner_name'  : child['owner']['name'],
			# 		'owner_email' : child['owner']['emailAddress'],
			# 		'owner_id'    : child['owner']['userId'],
			# 		'resolver_name'  : child['resolver']['name'],
			# 		'resolver_email' : child['resolver']['emailAddress'],
			# 		'resolver_id'    : child['resolver']['userId'],

			# 		'summary'    : child['summary'],
			# 		'description'    : child['description'],
			# 		'tags'    : child['tags'],
			# 	}

			# 	# prepare subscirbers attribute
			# 	subscription_list = ''
			# 	# print "this is the whole string of subscriptions:"
			# 	# print history['subscriptions']

			# 	# print "len of subscription is: " + str(len(history['subscriptions']))

			# 	# This block is necessary to convert subsriptions with single entry from OrderedDict to List
			# 	if isinstance(child['subscriptions'], dict):
			# 		# exit('bye bye')
			# 		all_subscriptions = [child['subscriptions']]
			# 	else:
			# 		all_subscriptions = child['subscriptions']

			# 	# print "type of all_subscriptions is: " + str(type(all_subscriptions))

			# 	for j, subscription in enumerate(all_subscriptions):
			# 		# print type(subscription)
			# 		# print "this is single subscription:"
			# 		# print subscription

			# 		if (j != 0):
			# 			subscription_list = subscription_list + "\n"
			# 		# print j
			# 		# print subscription
			# 		# print "type is " + str(type(subscription))
			# 		subscription_list = subscription_list + subscription['userId']

			# 	WI['subscribers'] = subscription_list


			# 	placeholders = ', '.join(['%s'] * len(WI))
			# 	columns = ', '.join(WI.keys())
			# 	sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (snapshot_table, columns, placeholders)
			# 	cursor.execute(sql, WI.values())

			# 	conn.commit()

			# 	print "success in story child insertion"

				# 	#insert comments of children
				# 	for child_comment in child['comments']:

		# print story


if __name__ == '__main__':
	# directory = input("Input location of json files: ")
	# getWorkitems()
	downloader()
