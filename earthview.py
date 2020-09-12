#!/bin/python
import urllib.request
import re
import sys
import os
import html
import sqlite3
import csv

class pics_infos_crawler():

	list_of_keys = ['id', 'name', 'slug', 'title', 'primaryColor', 'hue', 'lat', 'lng', 'country', 'region', 'attribution', 'photoUrl', 'thumbUrl', 'mapsLink', 'mapsLinkTitle', 'earthLink', 'earthLinkTitle', 'shareUrl', 'nextSlug', 'prevSlug']
	
	def __init__(self,db_conn):
		self.db_conn = db_conn
		self.cursor = self.db_conn.cursor()
		self.base_url = 'https://earthview.withgoogle.com/'
		self.initialize_database()
				
	def initialize_database(self):
		try:
			create_table_sql = '''CREATE TABLE IF NOT EXISTS pics_infos (
									id INTEGER PRIMARY KEY,
									name TEXT,
									slug TEXT,
									title TEXT,
									primaryColor TEXT,
									hue REAL NOT NULL,
									lat REAL NOT NULL,
									lng REAL NOT NULL,
									country TEXT,
									region	TEXT,
									attribution TEXT,
									photoUrl TEXT NOT NULL,
									thumbUrl TEXT,
									mapsLink TEXT,
									mapsLinkTitle TEXT,
									earthLink TEXT,
									earthLinkTitle TEXT,
									shareUrl TEXT,
									nextSlug TEXT,
									prevSlug TEXT)'''
			self.cursor.execute(create_table_sql)
		except sqlite3.Error as err:
			print(err, file=sys.stderr)
		

	def add_to_database(self,infos):
		try:
			columns_names = ', '.join(infos.keys())
			columns_content = ', '.join('?' * len(infos.keys()))
			query = 'INSERT INTO pics_infos (%s) VALUES (%s)' % (columns_names,columns_content)
			self.cursor.execute(query,list(infos.values()))
			self.db_conn.commit()
		except sqlite3.Error as err:
			print(err, file=sys.stderr)
	
	def slug_in_database(self,slug):
		self.cursor.execute("SELECT EXISTS(SELECT 1 FROM pics_infos WHERE slug=?)", (slug, ))
		result = self.cursor.fetchone()
		return (result != (0,))
		
	def get_infos_from_slug(self,slug): # The slug belongs to the database
		self.cursor.execute("SELECT * FROM pics_infos WHERE slug=?", (slug, ))
		result = self.cursor.fetchone()
		infos = dict(zip(self.list_of_keys,result))
		return infos
		
	def download_infos_from_slug(self,slug): # The informations are downloaded from the slug
		if slug == None:
			slug = ""
		with urllib.request.urlopen(self.base_url + slug.strip("\"")) as response:
			raw_data = html.unescape(response.read().decode('utf-8'))
			data = re.match(".*data-photo=\"\{([^\}]*)\}\".*", raw_data)
			if not data:
				print("No match of \'data-photo\' field. Exiting...", file=sys.stderr)
			processed = re.findall("(\".*?\":.*?),(?=\")",data[1]) # Ugly but working regex
			infos_list = [re.split(":(?!/)",p) for p in processed]
			infos = dict( (t[0].strip('\"'),t[1].strip('\"')) for t in infos_list)
#			infos["primaryColor"] = '"'+infos["primaryColor"]+'"' # Manual fix to avoid issues within SQL query
			return infos

	def get_next_unknown_slug(self,slug=None):
		if slug == None:
			slug = self.download_infos_from_slug(None)["slug"]#"olongapo-philippines-2180" # Hardcoded as starting slug
		if slug and not self.slug_in_database(slug):
			return slug
		starting_slug = slug
		current_slug = starting_slug
		keep_looking = True
		while keep_looking and self.slug_in_database(current_slug):
			infos = self.get_infos_from_slug(current_slug)
			next_slug = infos["nextSlug"]
			if next_slug == "":
				print("No next slug for slug "+current_slug+"...")
				return None
			if not self.slug_in_database(next_slug):
				return next_slug
			current_slug = next_slug
			if current_slug == starting_slug: # Avoid infinite loops
				keep_looking = False
		return None
				
	def download_picture(self,infos):
		if os.path.exists("./"+infos["id"].strip("\"")+".jpg"):
			return
		urllib.request.urlretrieve(infos["photoUrl"].strip("\""),"/."+infos["id"].strip("\"")+".jpg")
		
	def output_to_csv(self,output_file='./earthview.csv'):
		with open(output_file, "w", newline='') as csv_file:
			print("Updating CSV from database...")
			csv_writer = csv.writer(csv_file)
			csv_writer.writerow(self.list_of_keys)
			self.cursor.execute("SELECT * FROM pics_infos")
			rows = self.cursor.fetchall()
			count = 0
			for r in rows:
				csv_writer.writerow(r)
				count+=1
			print(str(count)+" entries have been written.")
		
	def input_from_csv(self,input_file='./earthview.csv'):
		with open(input_file,'r') as csv_file:
			reader = csv.reader(csv_file)
			print("Updating database from CSV...")
			proc_keys = ', '.join(self.list_of_keys)
			row_holder = ', '.join('?' * len(self.list_of_keys))
			count = 0
			for l in reader:
				try:
					if l == self.list_of_keys:
						continue # this is the first line
					query = 'INSERT INTO pics_infos (%s) VALUES (%s)' % (proc_keys,row_holder)
					self.cursor.execute(query,l)
					self.db_conn.commit()
					count+=1
				except sqlite3.Error as err:
					print(err, file=sys.stderr)
			print(str(count)+" entries have been processed.")
	
	def get_number_of_pics(self):
		self.cursor.execute("SELECT count(*) FROM pics_infos")
		return int(self.cursor.fetchone()[0])
		
if __name__ == "__main__":
	db_name = "./earthview.db"
	with sqlite3.connect(db_name) as db_conn:
		P = pics_infos_crawler(db_conn)
		P.initialize_database()
		P.input_from_csv()
		slug = P.get_next_unknown_slug()
		while slug:
			infos = P.download_infos_from_slug(slug)
			P.add_to_database(infos)
			#P.download_picture(infos)
			slug = P.get_next_unknown_slug()
		print("The database is complete! Total number of elements: "+str(P.get_number_of_pics()))
		print("Output all database in CSV format...")
		P.output_to_csv()
	db_conn.close()
