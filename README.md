# DAP_Crime_Analysis_LA

Group Project For Database & Analytics Programming (MSCDAD_C_JAN24I)

Group Members: Tejas Bafna(me) , Utkarsh Sharma and Shreyas Bhargav 


After processing MongoDB code generated CSV files are huge in size. Because of this all csv files are uploaded in google drive to access csv files
https://drive.google.com/drive/folders/1KsUptJ-tDaOU1klrJ8c5o3Hgoww4AwdY?usp=sharing


Steps to Follow to execute code:

1.Installation Docker
https://docs.docker.com/get-docker/

2.create a folder in your local system where u run docker containers

3.Put 3 files in a folder which given below:
  i. mongoDB.env
 ii. postgresql.env
iii. docker-compose.yml

4. Go to folder path in command prompt in which u created folder

5. Run a command
	docker-compose up

6. After that download postgreSQL(server) , pgadmin and mongodbcompass in your local system.

7. Create a new database in your PostgresSQL and in MongoDB

8. Then download DAP_project_crimeanalysis.py file in your system

9. Open your jupiter notebook or any other IDE's

10. Run requirement.txt using following command

	pip install -r requirement.txt

11. After all requirements installed just run .py final

