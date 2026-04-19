from marshmallow import Schema, fields


class EmptySchema(Schema):
    pass

class ConfigUpdateSchema(Schema):
    key = fields.String(required=True)
    value = fields.String(required=True)

# =========================
# RUN PIPELINE INPUT
# =========================
class RunPipelineSchema(Schema):
    include_static = fields.Boolean(required=False, metadata={
        "description": "Include static data (true/false)"
    })
# =========================
# STOP JOB INPUT
# =========================
class StopJobSchema(Schema):
    job_id = fields.Integer(required=False, metadata={
        "description": "Job ID (optional - if empty uses running job)"
    })

class JobQuerySchema(Schema):
    job_id = fields.Integer(required=False)

    
class AdAccountSchema(Schema):
    ad_account_id = fields.String(required=True, metadata={
        "description": "Meta Ad Account ID"
    })