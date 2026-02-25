<?php

namespace App;

final class Parser
{
    private const WORKERS = 4;
    private const READ_CHUNK = 1_048_576;
    private const WRITE_BUFFER = 1_048_576;
    private const PREFIX_LEN = 25;
    private const OVERHEAD = 51;
    private const DISCOVER_SIZE = 8_388_608;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        // Pre-generate all possible dates as integer-indexed lookup
        $dateIds = [];
        $dates = [];
        $dateCount = 0;

        for ($y = 2020; $y <= 2026; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $dim = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $ms = ($m < 10 ? '0' : '') . $m;
                for ($d = 1; $d <= $dim; $d++) {
                    $ds = $y . '-' . $ms . '-' . ($d < 10 ? '0' : '') . $d;
                    $dateIds[$ds] = $dateCount;
                    $dates[$dateCount] = $ds;
                    $dateCount++;
                }
            }
        }

        // Discover path slugs from first chunk
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $chunk = fread($handle, min($fileSize, self::DISCOVER_SIZE));
        fclose($handle);

        $lastNl = strrpos($chunk, "\n");
        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $pos = 0;

        while ($pos < $lastNl) {
            $nlPos = strpos($chunk, "\n", $pos + 50);
            $slug = substr($chunk, $pos + self::PREFIX_LEN, $nlPos - $pos - self::OVERHEAD);

            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }

            $pos = $nlPos + 1;
        }
        unset($chunk);

        // Chunk boundaries
        $nw = ($fileSize >= 10_000_000 && function_exists('pcntl_fork'))
            ? self::WORKERS
            : 1;

        $bounds = [0];
        if ($nw > 1) {
            $fh = fopen($inputPath, 'rb');
            for ($i = 1; $i < $nw; $i++) {
                fseek($fh, (int)($fileSize * $i / $nw));
                fgets($fh);
                $bounds[] = ftell($fh);
            }
            fclose($fh);
        }
        $bounds[] = $fileSize;

        if ($nw === 1) {
            $merged = self::scan(
                $inputPath, 0, $fileSize,
                $pathIds, $dateIds, $pathCount, $dateCount,
            );
        } else {
            $tmpDir = sys_get_temp_dir();
            $myPid = getmypid();
            $tmpFiles = [];
            $children = [];

            for ($i = 0; $i < $nw - 1; $i++) {
                $tf = $tmpDir . '/p_' . $myPid . '_' . $i;
                $tmpFiles[$i] = $tf;
                $cpid = pcntl_fork();

                if ($cpid === 0) {
                    $d = self::scan(
                        $inputPath, $bounds[$i], $bounds[$i + 1],
                        $pathIds, $dateIds, $pathCount, $dateCount,
                    );
                    file_put_contents($tf, pack('V*', ...$d));
                    exit(0);
                }

                $children[$i] = $cpid;
            }

            $merged = self::scan(
                $inputPath, $bounds[$nw - 1], $bounds[$nw],
                $pathIds, $dateIds, $pathCount, $dateCount,
            );

            foreach ($children as $cpid) {
                pcntl_waitpid($cpid, $st);
            }

            foreach ($tmpFiles as $tf) {
                $wc = unpack('V*', (string) file_get_contents($tf));
                unlink($tf);
                $j = 0;
                foreach ($wc as $v) {
                    $merged[$j++] += $v;
                }
            }
        }

        // JSON output — dates generated in chronological order, no sorting needed
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, self::WRITE_BUFFER);
        fwrite($out, '{');
        $first = true;

        foreach ($paths as $pathId => $slug) {
            $buf = $first ? '' : ',';
            $first = false;
            $escaped = str_replace('/', '\\/', $slug);
            $buf .= "\n    \"\/blog\/{$escaped}\": {";

            $base = $pathId * $dateCount;
            $sep = "\n";

            for ($di = 0; $di < $dateCount; $di++) {
                $c = $merged[$base + $di];
                if ($c === 0) {
                    continue;
                }
                $buf .= "{$sep}        \"{$dates[$di]}\": {$c}";
                $sep = ",\n";
            }

            $buf .= "\n    }";
            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    private static function scan(
        string $inputPath,
        int $start,
        int $end,
        array $pathIds,
        array $dateIds,
        int $pathCount,
        int $dateCount,
    ): array {
        $stride = $dateCount;
        $counts = array_fill(0, $pathCount * $stride, 0);

        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $start);

        $rem = $end - $start;
        $readSize = self::READ_CHUNK;
        $pLen = self::PREFIX_LEN;
        $oh = self::OVERHEAD;

        while ($rem > 0) {
            $chunk = fread($fh, $rem > $readSize ? $readSize : $rem);
            $cLen = strlen($chunk);
            $rem -= $cLen;

            $lastNl = strrpos($chunk, "\n");

            if ($lastNl === false) {
                fseek($fh, -$cLen, SEEK_CUR);
                $rem += $cLen;
                break;
            }

            if ($lastNl < $cLen - 1) {
                $excess = $cLen - $lastNl - 1;
                fseek($fh, -$excess, SEEK_CUR);
                $rem += $excess;
            }

            $pos = 0;

            while ($pos < $lastNl) {
                $nl = strpos($chunk, "\n", $pos + 50);

                $pid = $pathIds[substr($chunk, $pos + $pLen, $nl - $pos - $oh)] ?? -1;

                if ($pid >= 0) {
                    $counts[$pid * $stride + $dateIds[substr($chunk, $nl - 25, 10)]]++;
                }

                $pos = $nl + 1;
            }
        }

        fclose($fh);

        return $counts;
    }
}
