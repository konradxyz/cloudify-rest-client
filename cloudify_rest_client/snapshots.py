########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

import os
import urlparse
import contextlib

from cloudify_rest_client import bytes_stream_utils


class Snapshot(dict):

    def __init__(self, snapshot):
        self.update(snapshot)

    @property
    def id(self):
        return self.get('id')

    @property
    def created_at(self):
        return self.get('created_at')


class SnapshotsClient(object):

    def __init__(self, api):
        self.api = api

    def list(self, _include=None):
        response = self.api.get('/snapshots', _include=_include)
        return [Snapshot(item) for item in response]

    def create(self, snapshot_id):
        assert snapshot_id
        uri = '/snapshots/{0}'.format(snapshot_id)
        response = self.api.put(uri, expected_status_code=201)
        return Snapshot(response)

    def delete(self, snapshot_id):
        assert snapshot_id
        response = self.api.delete('/snapshots/{0}'.format(snapshot_id))
        return Snapshot(response)

    def restore(self, snapshot_id):
        assert snapshot_id
        uri = '/snapshots/{0}'.format(snapshot_id)
        self.api.post(uri, expected_status_code=201)

    def upload(self, snapshot_path, snapshot_id):
        assert snapshot_path
        assert snapshot_id

        uri = '/snapshots/{0}/upload'.format(snapshot_id)
        query_params = {}

        if urlparse.urlparse(snapshot_path).scheme and \
                not os.path.exists(snapshot_path):
            query_params['snapshot_archive_url'] = snapshot_path
            data = None
        else:
            data = bytes_stream_utils.request_data_file_stream_gen(
                snapshot_path)

        response = self.api.put(uri, params=query_params, data=data,
                                expected_status_code=201)
        return Snapshot(response)

    def download(self, snapshot_id, output_file):
        uri = '/snapshots/{0}/download'.format(snapshot_id)

        with contextlib.closing(self.api.get(uri, stream=True)) as response:
            output_file = bytes_stream_utils.write_response_stream_to_file(
                response, output_file)

            return output_file
