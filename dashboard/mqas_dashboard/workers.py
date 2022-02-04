from .auth import auth
from flask import Blueprint, redirect, render_template, request, url_for
from .db import get_db

bp = Blueprint('workers', __name__, url_prefix='/workers')

@bp.route('/', methods=('GET', 'POST'))
@auth.login_required
def items():
  db = get_db()
  workers = db.get_workers()
  if request.method == 'GET':
    return render_template('workers/list.html', workers=workers)


@bp.route('/<workerid>', methods=('GET', 'POST'))
@auth.login_required
def item(workerid):
  db = get_db()
  worker = db.get_worker(worker_id=workerid)
  if worker is None:
    return redirect(url_for('workers.items'))

  if request.method == 'GET':
    attrs = [
      dict(text="ID", value=worker.get("_id")),
      dict(text="System Info", value=worker.get("info")),
      dict(text="Status", value=worker.get("status")),
      dict(text="Queues", value=worker.get("queues")),
      dict(text="Completed Jobs", value=worker.get("jobs_completed")),
      dict(text="Failed Jobs", value=worker.get("jobs_failed")),
      dict(text="Created At", value=worker.get("createdAt")),
      dict(text="Updated At", value=worker.get("updatedAt")),
    ]

    last_error = worker.get("last_error_message")
    errors = []
    if not last_error is None:
      if not isinstance(last_error, dict):
        last_error = dict(message=last_error, trace="")

      errors = [
        dict(text="Job Id", value=last_error.get("jobId"), cssclass="w3-text-red"),
        dict(text="Message", value=last_error.get("message"), cssclass="w3-text-red"),
        dict(text="Occurred At", value=last_error.get("errorAt"), cssclass="w3-text-red"),
        dict(text="Trace", value=last_error.get("trace"), cssclass="w3-text-red"),
        dict(text="Job", value=last_error.get("callback"), cssclass="w3-text-red"),
      ]

    return render_template('workers/item.html', worker=worker, attributes=attrs, errors=errors, has_errors=len(errors) > 0)
