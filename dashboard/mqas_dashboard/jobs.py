from datetime import datetime
from math import ceil
from .auth import auth
from flask import Blueprint, redirect, render_template, request, url_for

from .db import get_db

bp = Blueprint('jobs', __name__, url_prefix='/jobs')

def parse_date(dt, is_end=False):
  sp = str(dt).split('-')
  if len(sp) >= 3:
    if is_end:
      return datetime(int(sp[0]), int(sp[1]), int(sp[2]), 23, 59, 59, 999)
    else:
      return datetime(int(sp[0]), int(sp[1]), int(sp[2]), 0, 0, 0, 0)

  return None

@bp.route('/', methods=('GET', 'POST'))
@auth.login_required
def items():
  db = get_db()
  jobs=[]
  total=0

  options = dict(
    txt="",
    df="",
    dt="",
    lang="all",
    status="all",
    limit=10,
    page=1,
    total=0,
    numOfPages=1
  )

  if request.method=="POST":
    fd = request.form
    options["txt"] = fd.get("searchText", "")
    options["df"] = fd.get("periodFrom", "")
    options["dt"] = fd.get("periodTo", "")
    options["lang"] = fd.get("language", "all")
    options["status"] = fd.get("status", "all")
    options["limit"] = fd.get("limit", 0)
    options["page"] = fd.get("page", 1)

    if options["page"] in ["<" , "<<", ">", ">>"]:
      cp = int(fd.get("currentPage", 1))
      nps = int(fd.get("numOfPages", 1))
      if options["page"] == "<":
        options["page"] = max(1, cp - 1)
      elif options["page"] == "<<":
        options["page"] = 1
      elif options["page"] == ">":
        options["page"] = min(cp + 1, nps)
      elif options["page"] == ">>":
        options["page"] = nps
    
    options["page"] = int(options["page"])

  query = {}

  if options["df"] != "" or options["dt"] != "":
    query["createdAt"] = {}
    if options["df"] != "":
      d = parse_date(options["df"], False)
      if not d is None:
        query["createdAt"]["$gte"] = d
    if options["dt"] != "":
      d = parse_date(options["dt"], True)
      if not d is None:
        query["createdAt"]["$lte"] = d

  if options["lang"] != "all":
    query["lang"] = options["lang"]

  if options["status"] != "all":
    if options["status"] == "error":
      query["error"] = True
    elif options["status"] == "in-progress":
      query["inProgress"] = True
    elif options["status"] == "done":
      query["done"] = True
    elif options["status"] == "pending":
      query["done"] = {"$ne": True}
      query["error"] = {"$ne": True}
      query["inProgress"] = {"$ne": True}
  
  if options["txt"] != "":
    query["$or"] = [
      {"data.function_name": {"$regex": options["txt"], "$options": "i"}},
      {"lang": {"$regex": options["txt"], "$options": "i"}},
      {"channel": {"$regex": options["txt"], "$options": "i"}}
    ]

  skip = 0
  if options["limit"] != "all":
    skip = (options["page"] - 1) * int(options["limit"])
  
  jobs, total = db.get_jobs(query=query, limit=options["limit"], skip=skip)
  options["total"] = total
  options["numOfPages"] = 1 if options['limit'] == "all" else ceil(int(total) / int(options["limit"]))

  for job in jobs:
    job["job_name"] = job.get("data", {}).get("function_name")
    job["status"] = "pending"

    if job.get("error"):
      job["status"] = "failed"
    elif job.get("done"):
      job["status"] = "completed"
    elif job.get("inProgress"):
      job["status"] = "in progress"

  options["paging"] = {"left": range(max(1, options['page'] - 5), options['page']), "right": range(options['page']+1, min(options["numOfPages"], options['page'] + 5)+1)}
  return render_template('jobs/list.html', jobs=jobs, options=options)


@bp.route('/<jobid>', methods=('GET', 'POST'))
@auth.login_required
def item(jobid):
  db = get_db()
  job = db.get_job(job_id=jobid)
  if job is None:
    return redirect(url_for('jobs.items'))

  job["job_name"] = job.get("data", {}).get("function_name")
  job["status"] = "pending"

  if job.get("error"):
    job["status"] = "failed"
  elif job.get("done"):
    job["status"] = "completed"
  elif job.get("inProgress"):
    job["status"] = "in progress"

  if request.method == 'GET':
    attrs = [
      dict(text="ID", value=job.get("_id")),
      dict(text="Callback", value=job.get("job_name")),
      dict(text="Status", value=job.get("status")),
      dict(text="Channel", value=job.get("channel")),
      dict(text="Language", value=job.get("lang")),
      dict(text="Created At", value=job.get("createdAt")),
      dict(text="Expires At", value=job.get("expireAt")),
    ]

    last_error = job.get("errorMessage")
    errors = []
    
    if job["status"] == "failed" and not last_error is None:
      if not isinstance(last_error, dict):
        last_error = dict(message=last_error, trace="")

      errors = [
        dict(text="Message", value=last_error.get("message"), cssclass="w3-text-red"),
        dict(text="Trace", value=last_error.get("trace"), cssclass="w3-text-red")
      ]

    args = job.get("data", {}).get("args", [])
    k_wargs = job.get("data", {}).get("kwargs", {})
    kwargs = []
    for k in k_wargs:
      kwargs.append(dict(text=k, value=k_wargs.get(k)))

    res = job.get("result")
    has_results = False
    results = []
    if not res is None:
      has_results = True
      if isinstance(res, dict):
        for k in res:
          results.append(dict(text=k, value=res.get(k)))
      elif isinstance(res, list) or isinstance(res, tuple):
        for k, v in enumerate(res):
          results.append(dict(text=k, value=v))
      else:
        results.append(dict(text="", value=res))

    return render_template('jobs/item.html', job=job, attributes=attrs, errors=errors, has_errors=len(errors) > 0, args=args, has_args=len(args) > 0, kwargs=kwargs, has_kwargs=len(kwargs) > 0, has_results=has_results, results=results)


@bp.route('/<jobid>/requeue', methods=['POST',])
@auth.login_required
def requeue(jobid):
  db = get_db()
  job = db.get_job(job_id=jobid)

  if not job is None:
    db.requeue_job(job_id=jobid)
  
  return redirect(url_for('jobs.items'))


@bp.route('/<jobid>/remove', methods=['POST',])
@auth.login_required
def remove(jobid):
  db = get_db()
  job = db.get_job(job_id=jobid)

  if not job is None:
    db.remove_job(job_id=jobid)
  
  return redirect(url_for('jobs.items'))
  