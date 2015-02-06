# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
