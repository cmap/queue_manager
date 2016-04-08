delete from queue_type;
insert into queue_type (name) values ("monitor");
insert into queue_type (name) values ("get");
insert into queue_type (name) values ("P100 QC+NORM");
insert into queue_type (name) values ("P100 QC");
insert into queue_type (name) values ("P100 DIA");
insert into queue_type (name) values ("GCP QC+NORM");
insert into queue_type (name) values ("GCP QC");
insert into queue_type (name) values ("put");

delete from workflow_template;
insert into workflow_template (name) values ("P100");
insert into workflow_template (name) values ("GCP");
insert into workflow_template (name) values ("P100 QC");
insert into workflow_template (name) values ("GCP QC");

delete from workflow_template_pair;
insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="P100" and qt1.name="monitor" and qt2.name="get";
insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="P100" and qt1.name="get" and qt2.name="P100 QC+NORM";
insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="P100" and qt1.name="P100 QC+NORM" and qt2.name="put";

insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="GCP" and qt1.name="monitor" and qt2.name="get";
insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="GCP" and qt1.name="get" and qt2.name="GCP QC+NORM";
insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="GCP" and qt1.name="GCP QC+NORM" and qt2.name="put";

insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="P100 QC" and qt1.name="monitor" and qt2.name="get";
insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="P100 QC" and qt1.name="get" and qt2.name="P100 QC";
insert into workflow_template_pair (workflow_template_id, prev_queue_type_id, next_queue_type_id)
    select wf.id, qt1.id, qt2.id
    from workflow_template wf, queue_type qt1, queue_type qt2
    where wf.name="P100 QC" and qt1.name="P100 QC" and qt2.name="put";
