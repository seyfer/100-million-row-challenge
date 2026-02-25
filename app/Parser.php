<?php

namespace App;

final class Parser
{
    private const NUM_WORKERS = 2;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        if ($fileSize < 10 * 1024 * 1024 || !function_exists('pcntl_fork')) {
            $data = $this->processChunk($inputPath, 0, $fileSize);
            $this->writeOutput($data, $outputPath);
            return;
        }

        $numWorkers = self::NUM_WORKERS;

        $fh = fopen($inputPath, 'rb');
        $boundaries = [0];

        for ($i = 1; $i < $numWorkers; $i++) {
            fseek($fh, (int)($fileSize * $i / $numWorkers));
            fgets($fh);
            $boundaries[] = ftell($fh);
        }

        $boundaries[] = $fileSize;
        fclose($fh);

        $tmpFiles = [];
        $pids = [];

        for ($w = 1; $w < $numWorkers; $w++) {
            $tmpFiles[$w] = tempnam(sys_get_temp_dir(), 'parser');
            $pid = pcntl_fork();

            if ($pid === 0) {
                $childData = $this->processChunk($inputPath, $boundaries[$w], $boundaries[$w + 1]);
                file_put_contents($tmpFiles[$w], serialize($childData));
                exit(0);
            }

            $pids[$w] = $pid;
        }

        $data = $this->processChunk($inputPath, $boundaries[0], $boundaries[1]);

        for ($w = 1; $w < $numWorkers; $w++) {
            pcntl_waitpid($pids[$w], $status);
            $childData = unserialize(file_get_contents($tmpFiles[$w]));
            unlink($tmpFiles[$w]);

            foreach ($childData as $path => $dates) {
                if (isset($data[$path])) {
                    foreach ($dates as $date => $count) {
                        $data[$path][$date] = ($data[$path][$date] ?? 0) + $count;
                    }
                } else {
                    $data[$path] = $dates;
                }
            }
        }

        unset($childData);
        $this->writeOutput($data, $outputPath);
    }

    private function writeOutput(array &$data, string $outputPath): void
    {
        foreach ($data as &$dates) {
            ksort($dates);
        }
        unset($dates);

        $fp = fopen($outputPath, 'wb');
        stream_set_write_buffer($fp, 1 << 20);

        $buf = "{\n";
        $firstPath = true;

        foreach ($data as $path => $dates) {
            if (!$firstPath) {
                $buf .= ",\n";
            }
            $firstPath = false;

            $buf .= '    "' . str_replace('/', '\\/', $path) . "\": {\n";

            $firstDate = true;
            foreach ($dates as $date => $count) {
                if (!$firstDate) {
                    $buf .= ",\n";
                }
                $firstDate = false;
                $buf .= '        "' . $date . '": ' . $count;
            }

            $buf .= "\n    }";

            if (strlen($buf) > 1048576) {
                fwrite($fp, $buf);
                $buf = '';
            }
        }

        $buf .= "\n}";
        fwrite($fp, $buf);
        fclose($fp);
    }

    private function processChunk(string $inputPath, int $start, int $end): array
    {
        $data = [];
        $fh = fopen($inputPath, 'rb');

        if ($start > 0) {
            fseek($fh, $start);
        }

        $remaining = $end - $start;
        $bufferSize = 2 * 1024 * 1024;
        $remainder = '';

        while ($remaining > 0) {
            $readSize = min($bufferSize, $remaining);
            $chunk = fread($fh, $readSize);

            if ($chunk === false || $chunk === '') {
                break;
            }

            $remaining -= strlen($chunk);

            if ($remainder !== '') {
                $chunk = $remainder . $chunk;
                $remainder = '';
            }

            $lastNewline = strrpos($chunk, "\n");

            if ($lastNewline === false) {
                $remainder = $chunk;
                continue;
            }

            if ($lastNewline < strlen($chunk) - 1) {
                $remainder = substr($chunk, $lastNewline + 1);
            }

            $pos = 0;

            while ($pos < $lastNewline) {
                $commaPos = strpos($chunk, ',', $pos);
                $path = substr($chunk, $pos + 19, $commaPos - $pos - 19);
                $date = substr($chunk, $commaPos + 1, 10);

                if (isset($data[$path][$date])) {
                    $data[$path][$date]++;
                } else {
                    $data[$path][$date] = 1;
                }

                $pos = $commaPos + 27;
            }
        }

        if ($remainder !== '') {
            $commaPos = strpos($remainder, ',');
            $path = substr($remainder, 19, $commaPos - 19);
            $date = substr($remainder, $commaPos + 1, 10);

            if (isset($data[$path][$date])) {
                $data[$path][$date]++;
            } else {
                $data[$path][$date] = 1;
            }
        }

        fclose($fh);

        return $data;
    }
}
