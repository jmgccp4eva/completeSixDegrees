# Phase One: 
## Data Collection
This is the first phase of what could be an extensive analytics for the Kevin Bacon game.

For an explanation of The Kevin Bacon Game, visit [Wikipedia](https://en.wikipedia.org/wiki/Six_Degrees_of_Kevin_Bacon#:~:text=Six%20Degrees%20of%20Kevin%20Bacon%20or%20Bacon's%20Law%20is%20a,ultimately%20leads%20to%20prolific%20American)

A brief explanation is that Kevin Bacon appeared in Movie1 with Actor1 who appeared in Movie2 with Actor2...Actor 5 appeared in Movie6 with Actor6.  Each level of Actor has a specific Kevin Bacon Number(KBN) showing the are X number of degrees away from Kevin Bacon.

For example, **Kevin Bacon** appeared in *Footloose* with **John Lithgow**, who appeared in *Ricochet* with **Denzel Washington**, who appeared in *Training Day* with **Tom Berenger**, who appeared in *Major League* with **Rene Russo**, who appeared in *Ransom* with **Gary Sinise**, who appeared in *Mission to Mars* with **Kim Delaney**.

This means that all of these actors have KBNs, but this does not necessarily mean these are their true KBN.

For example, **Gary Sinise** has a KBN in the example as 5.  However, both he and **Kevin Bacon** appeared in *Apollo 13*, which means **Gary Sinise**'s true KBN is 1.

** NOTE: ** This repository is strictly the data collection phase.  

After the data collection, there will be a few other repos that will be linked in this Readme file for:
The actual processing of KBN in Python using Pandas, SQL, Neo4j
A comparison of speeds based off of Pandas vs SQL vs Neo4j
A modified KBN for TV shows and movies, displaying how many numbers each actor is from a particular TV Show or Movie
