<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Storage;

class Destinations extends Model
{
    use HasFactory;

    protected static function booted(): void
    {
        static::deleting(function (self $destination): void {
            if ($destination->filename) {
                Storage::disk('local')->delete($destination->filename);
            }
        });
    }

    /** ─────────────────────────────────────────────────────────────
     *  Table & primary-key settings
     * ───────────────────────────────────────────────────────────── */
    protected $table = 'destinations';
    protected $primaryKey = 'id';

    /** ─────────────────────────────────────────────────────────────
     *  Mass-assignment: whitelist all but the PK & timestamps
     * ───────────────────────────────────────────────────────────── */
    protected $fillable = [
        'destination_name',
        'job_type',
        'status',
        'filename',
        'date',
        'minimum_reviews',
        'city',
        'country',
        'start_date',
        'end_date',
        'months',
        'temp_run_id',
        'error_message',
    ];

    /** ─────────────────────────────────────────────────────────────
     *  Attribute casting
     * ───────────────────────────────────────────────────────────── */
    protected $casts = [
        'date'            => 'datetime',      // Carbon instance
        'minimum_reviews' => 'integer',
        'start_date'      => 'date',
        'end_date'        => 'date',
        'months'          => 'integer',
    ];

    /**
     * Optional: cast “status” into an Enum for stronger typing.
     *  1.  php artisan make:enum DestinationStatus
     *  2.  populate enum with values: NEW, QUEUED, PROCESSING, DONE, ERROR
     *  3.  Uncomment the line below.
     */
    // protected $casts = [
    //     'status' => DestinationStatus::class,
    //     'date'   => 'datetime',
    //     'minimum_reviews' => 'integer',
    // ];

    /** ─────────────────────────────────────────────────────────────
     *  Convenience scopes
     * ───────────────────────────────────────────────────────────── */

    /** Only jobs awaiting processing */
    public function scopeQueued($query)
    {
        return $query->where('status', 'queued');
    }

    /** Only jobs completed successfully */
    public function scopeDone($query)
    {
        return $query->where('status', 'done');
    }

    /** Mark as processed and store Excel filename */
    public function markDone(string $filename): void
    {
        $this->update([
            'status'   => 'done',
            'filename' => $filename,
            'date'     => Carbon::now(),
        ]);
    }
}
