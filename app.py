import json
import psycopg2
import os
from googletrans import Translator
import emoji
from datetime import datetime
import datetime as dt
from errno import errorcode
import config

DB = psycopg2.connect(options=f'-c search_path={config.options}', database=config.database, user=config.user,
                      password=config.password, host=config.host, port=config.port)
MY_CURSOR = DB.cursor()

trans = Translator()


def parse(account):
    os.system(
        f'instagram-scraper {account} -m 100 --retry-forever --comments')#-u {config.instagram_user} -p {config.instagram_password}


def read_json(account):
    with open(f'{account}\\{account}.json', 'r', encoding='utf-8') as f:
        text = json.load(f)
    comments = []
    posts = []

    for txt in text['GraphImages']:
        taken_at_timestamp = datetime.fromtimestamp(txt['taken_at_timestamp'])
        two_day_before = datetime.today() - dt.timedelta(days=2)
        if taken_at_timestamp >= two_day_before:
            d = txt['comments']

            post_id = txt['id']
            display_url = txt['display_url']
            edge_media_to_caption = txt['edge_media_to_caption']
            edges = edge_media_to_caption['edges']
            desc_post = edges[0]

            post = ({
                'id': post_id,
                'description': desc_post['node']['text'],
                'display_url': display_url,
                'release_post': taken_at_timestamp,
            })
            posts.append(post)


            for i in d['data']:
                comment = ({'id': i['id'],
                            'post_id': post_id,
                            'owner_id': i['owner']['id'],
                            'username': i['owner']['username'],
                            'comment_text': i['text'],
                            'created_at': datetime.fromtimestamp(i['created_at']),
                            })
                comments.append(comment)

        return posts, comments


def save_posts(posts, account):
    for post in posts:
        try:
            sql_formula = f'INSERT INTO ig_popular_accs_posts(id, description, ig_account, display_url, created_at) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING'
            content = (
                [post['id'], post['description'], account, post['display_url'], post['release_post']])

            MY_CURSOR.execute(sql_formula, content)
        except:
            pass
        DB.commit()


def save_comments(comments, account):
    for i in comments:
        try:
            sql_formula = f'INSERT INTO ig_popular_accs_comments( id ,post_id, owner_id, ig_account,username, comment_text, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING'

            content = ([i['id'], i['post_id'], i['owner_id'], account, i['username'], i['comment_text'], i['created_at']])
            MY_CURSOR.execute(sql_formula, content)
        except:
            pass
        DB.commit()


def find_deleted_messages(items, account):
    MY_CURSOR.execute(f"UPDATE ig_popular_accs_comments SET deleted=TRUE where ig_account='{account}'")
    for i in items:
        MY_CURSOR.execute(f"UPDATE ig_popular_accs_comments SET deleted=FALSE where id='{i['id']}' and ig_account='{account}'")
        DB.commit()


def translate():
    MY_CURSOR.execute('select * from ig_popular_accs_posts where description_ru is null or description_en is null')
    posts = MY_CURSOR.fetchall()
    for ps in posts:
        strin = ps[1]
        no_emoji = emoji.get_emoji_regexp().sub(u'', strin)
        description_ru = trans.translate(no_emoji, dest='ru')
        description_en = trans.translate(no_emoji, dest='en')
        dosca = description_en.text.replace("'", '"')
        MY_CURSOR.execute(
            f"update ig_popular_accs_posts set description_ru='{description_ru.text}', description_en='{dosca}' where id={ps[0]}")
        DB.commit()

    MY_CURSOR.execute('select * from ig_popular_accs_comments where comment_text_ru is null or comment_text_en is null')
    coments = MY_CURSOR.fetchall()
    for com in coments:
        strin = com[4]
        no_emoji = emoji.get_emoji_regexp().sub(u'', strin)
        comment_text_ru = trans.translate(no_emoji, dest='ru')
        comment_text_en = trans.translate(no_emoji, dest='en')
        dosca = comment_text_en.text.replace("'", '"')
        MY_CURSOR.execute(
            f"update ig_popular_accs_comments set comment_text_ru='{comment_text_ru.text}', comment_text_en='{dosca}' where id={com[0]}")
        DB.commit()


def main():
    for i in range(len(config.accounts)):
        try:
            parse(config.accounts[i])

            read_json(config.accounts[i])

            posts, comments = read_json(config.accounts[i])

            MY_CURSOR.execute('create table if not exists ig_popular_accs_posts(id bigint not null constraint post_pk primary key, description text, ig_account text, display_url text, description_ru text default null, description_en text default null, created_at timestamp)')
            save_posts(posts, config.accounts[i])

            MY_CURSOR.execute('create table if not exists ig_popular_accs_comments(id bigserial not null constraint comment_pkey primary key, post_id bigint not null constraint comment_post_id_fk references ig_popular_accs_posts, owner_id bigint not null, ig_account varchar(30) not null, username varchar(30) not null, comment_text text not null, deleted boolean default false, created_at timestamp, comment_text_ru text default null, comment_text_en text default null)')
            save_comments(comments, config.accounts[i])

            find_deleted_messages(comments, config.accounts[i])

            # translate()
        except:
            pass
main()