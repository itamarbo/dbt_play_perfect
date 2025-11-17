Welcome to your new dbt project!

### Using the starter project

Try running the following commands:

- dbt run
- dbt test

### Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

🎯 מודל A: fct_games (שורה = משחק ייחודי לשחקן) 🎮
מודל זה הוא ברמת פירוט (Grain) של שחקן + חדר. המטרה היא לרכז את כל המידע שקשור להשתתפות שחקן ספציפי בחדר ספציפי.

🎯 מודל B: daily_player (שורה = סיכום יומי לשחקן) 📅
מודל זה הוא ברמת פירוט (Grain) של שחקן + יום. המטרה היא סיכום כל הפעילות של השחקן ביום נתון.

🎯 מודל C: fct_rooms (שורה = סיכום חדר שלם) 🏟️
מודל זה הוא ברמת פירוט (Grain) של חדר (Room). המטרה היא סיכום כלכלי וסטטיסטי של החדר כולו.

dbt docs generate
dbt docs serve
