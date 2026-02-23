<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('destinations', function (Blueprint $table) {
            $table->string('job_type')->default('permanent')->after('destination_name');
            $table->string('city')->nullable()->after('minimum_reviews');
            $table->string('country')->nullable()->after('city');
            $table->date('start_date')->nullable()->after('country');
            $table->date('end_date')->nullable()->after('start_date');
            $table->unsignedInteger('months')->nullable()->after('end_date');
            $table->string('temp_run_id')->nullable()->after('months');
            $table->text('error_message')->nullable()->after('temp_run_id');

            $table->index(['job_type', 'status']);
            $table->index('temp_run_id');
        });
    }

    public function down(): void
    {
        Schema::table('destinations', function (Blueprint $table) {
            $table->dropIndex(['job_type', 'status']);
            $table->dropIndex(['temp_run_id']);

            $table->dropColumn([
                'job_type',
                'city',
                'country',
                'start_date',
                'end_date',
                'months',
                'temp_run_id',
                'error_message',
            ]);
        });
    }
};
