from contextlib import nested
from jinja2 import Template

def render_config(template_location, rendered_location, properties):
  """
  A method for rendering simple key/value configs into a template. Uses Jinja2
  style templating.

  param: template_location -- File path of the input Jinja2 template.
  param: rendered_location -- File path where rendered output should be saved.
  param: properties -- A dictionary of key/value pairs to be passed to the
  template with the accessor name 'properties'.
  """
  with nested(open(template_location, 'r'), open(rendered_location, 'w')) as (input, output):
    template = Template(input.read())
    rendered = template.render(properties=properties)
    output.write(rendered)
