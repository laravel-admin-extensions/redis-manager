@extends('laravel-admin-redis-manager::layout')

@section('page')

<div class="box box-primary">
    <div class="box-header with-border">
        <h3 class="box-title">Edit</h3> <small></small>
    </div>

    <form class="form-horizontal">
        <div class="box-body">
            <div class="form-group">
                <label for="inputKey" class="col-sm-2 control-label">Key</label>

                <div class="col-sm-10">
                    <input type="text" class="form-control key" name="key" id="inputKey" placeholder="key" readonly value="{{ $data['key'] ?? '' }}">
                </div>
            </div>
            <div class="form-group">
                <label for="inputValue" class="col-sm-2 control-label">Value</label>

                <div class="col-sm-10">
                    <textarea class="form-control value" id="inputValue" rows="6" name="value">{{ $data['value'] ?? '' }}</textarea>
                </div>
            </div>
            <div class="form-group">
                <label for="inputExpire" class="col-sm-2 control-label">Expire</label>

                <div class="col-sm-10">
                    <input type="number" class="form-control expire" id="inputExpire" name="ttl" value="{{ $data['ttl'] ?? -1 }}">
                </div>
            </div>

            {{ csrf_field() }}
            <input type="hidden" name="conn" value="{{ $conn }}">
            <input type="hidden" name="type" value="string">
            <input type="hidden" name="_method" value="put">
        </div>
        <!-- /.box-body -->

        <div class="box-footer">

            <div class="col-sm-offset-2">
            <button type="reset" class="btn btn-default pull-right">Reset</button>
            <button class="btn btn-info update-string">Submit</button>
            </div>
        </div>

    </form>

</div>
<!-- /.box-body -->

<script>
    $(function () {
        $('.update-string').on('click', function (event) {
            event.preventDefault();

            var key = $('input.key').val();
            var value = $('textarea.value').val();
            var ttl = $('input.expire').val();

            if (key == '' || value == '') {
                return;
            }

            var params = {
                key: key,
                value: value,
                conn: "{{ $conn }}",
                ttl: ttl,
                type:'string',
                _token: LA.token
            };

            $.ajax({
                url: '{{ route('redis-update-key') }}',
                type: 'PUT',
                data: params,
                success: function(result) {
                    toastr.success('Add success.');
                    $.pjax.reload('#pjax-container');
                }
            });

        });
    })
</script>

@endsection
