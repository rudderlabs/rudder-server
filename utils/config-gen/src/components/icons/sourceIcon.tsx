import React from 'react';
import { ReactComponent as Android } from '@svg/android.svg';
import { ReactComponent as Ios } from '@svg/ios.svg';
import { ReactComponent as Javascript } from '@svg/javascript.svg';
import { ReactComponent as Unity } from '@svg/unity.svg';
import theme from '@css/theme';

const SourceIcon = (props: any) => {
  let height = props.height || theme.iconSize.medium;
  let width = props.width || theme.iconSize.medium;
  switch (props.source.toLowerCase()) {
    case 'android':
      return <Android width={width} height={height} />;
    case 'ios':
      return <Ios width={width} height={height} />;
    case 'javascript':
      return <Javascript width={width} height={height} />;
    case 'unity':
      return <Unity width={width} height={height} />;
    default:
      break;
  }
  return <div />;
};

export default SourceIcon;
