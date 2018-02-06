from evaluations.evaluate import *
import time
import click
import json

def welcome():
	print ("\
 _________     _____    _________ _________.___________ _____________________.___   _____ \n \
\_   ___ \   /  _  \  /   _____//   _____/|   \_____  \\\______   \_   _____/|   | /  _  \  \n \
/    \  \/  /  /_\  \ \_____  \ \_____  \ |   |/   |   \|     ___/|    __)_ |   |/  /_\  \ \n \
\     \____/    |    \/        \/        \|   /    |    \    |    |        \|   /    |    \ \n \
 \______  /\____|__  /_______  /_______  /|___\_______  /____|   /_______  /|___\____|__  / \n \
        \/         \/        \/        \/             \/                 \/             \/ ")


@click.command()
@click.option('--conf_file', prompt ='Conf file path?')

def main(conf_file):
	then = time.time()
	welcome()
	conf = json.load(open(conf_file))
	evaluateS3(conf)
	evaluateStaging(conf)
	evaluateDWH(conf)
	evaluateOLAP(conf)
	sendAllResultsToS3(conf)
	loadResultsToReshift(conf)
	now = time.time() 
	print("It took: ", now-then, " seconds")

if __name__ == '__main__':
    main()