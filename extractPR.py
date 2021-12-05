from github import Github, RateLimitExceededException, BadCredentialsException, BadAttributeException, \
    GithubException, UnknownObjectException, BadUserAgentException
import pandas as pd
import requests
import time
import datetime

access_token = "access_token"

def extract_project_PRs(project_full_name):
    df_PRs = pd.DataFrame()
    while True:
        try:
            g = Github(access_token, retry=10, timeout=15, per_page=100)
            print(f'Extracting data from {project_full_name} repo')
            repo = g.get_repo(project_full_name)
            PRs_list = repo.get_pulls(state='open', sort='created', base='master')

            for pr in PRs_list:
                try:
                    print(g.rate_limiting)
                    print(f'Extracting data from PR # {pr.number}')
                    df_PRs = df_PRs.append({
                        'pr_id': pr.id,
                        'pr_title': pr.title,
                        'pr_created_at': pr.created_at,
                        'opened_for': datetime.datetime.now()- pr.created_at,

                    }, ignore_index=True)
                except RateLimitExceededException as e:
                    print(e.status)
                    print('Rate limit exceeded')
                    time.sleep(300)
                    continue
                except BadCredentialsException as e:
                    print(e.status)
                    print('Bad credentials exception')
                    break
                except UnknownObjectException as e:
                    print(e.status)
                    print('Unknown object exception')
                    break
                except GithubException as e:
                    print(e.status)
                    print('General exception')
                    break
                except requests.exceptions.ConnectionError as e:
                    print('Retries limit exceeded')
                    print(str(e))
                    time.sleep(10)
                    continue
                except requests.exceptions.Timeout as e:
                    print(str(e))
                    print('Time out exception')
                    time.sleep(10)
                    continue

        except RateLimitExceededException as e:
            print(e.status)
            print('Rate limit exceeded')
            time.sleep(300)
            continue
        except BadCredentialsException as e:
            print(e.status)
            print('Bad credentials exception')
            break
        except UnknownObjectException as e:
            print(e.status)
            print('Unknown object exception')
            break
        except GithubException as e:
            print(e.status)
            print('General exception')
            break
        except requests.exceptions.ConnectionError as e:
            print('Retries limit exceeded')
            print(str(e))
            time.sleep(10)
            continue
        except requests.exceptions.Timeout as e:
            print(str(e))
            print('Time out exception')
            time.sleep(10)
            continue
        break
    df_PRs.to_csv('Dataset/PRs_dataset.csv', sep=',', encoding='utf-8', index=True)

extract_project_PRs('gsdevops/deployer')