import { Button as AntDButton } from 'antd';
import styled from 'styled-components';
import { is, isNot } from 'typescript-styled-is';

export const Button = styled(AntDButton) <any>`
  &&& {
    box-shadow: 0px 1px 6px rgba(0, 0, 0, 0.15);
    border-radius: 40px;
    border: none;
    padding: 0 20px;
    height: ${props => (props.compact ? '32px' : '48px')};
    font-size: ${({ theme }) => theme.fontSize.md};
    font-weight: ${({ theme }) => theme.fontWeight.semiBold};
    text-transform: ${props => (props.smallcase ? 'none' : 'uppercase')};
    color: ${({ theme }) => theme.color.primary};
    background-color: ${props => (props.color)};
    width: ${props => (props.width)};
    line-height: 22px;
    display: flex;
    align-items: center;
    justify-content: center;

    svg {
      margin-right: 10px;
      path {
        fill: ${({ theme }) => theme.color.primary};
      }
      &[data-icon='loading'] {
        margin-right: 0px;
      }
    }
    &:hover {
      background-color: ${({ theme }) => theme.color.primary200};
      color: ${({ theme }) => theme.color.white};
      svg path {
        fill: ${({ theme }) => theme.color.white};
      }
    }
  }
`;

export const ButtonPrimary = styled(Button).attrs({
  type: 'primary',
})`
  &&& {
    background-color: ${({ theme }) => theme.color.primary};
    color: ${({ theme }) => theme.color.white};
    &:hover {
      background-color: ${({ theme }) => theme.color.primary200};
    }
    svg[data-icon='loading'] {
      color: ${({ theme }) => theme.color.white};
      margin-right: 0px;
      path {
        fill: ${({ theme }) => theme.color.white};
      }
    }
  }
`;

export const ButtonSecondary = styled(Button)`
  &&& {
    background-color: ${({ theme }) => theme.color.white};
    color: ${({ theme }) => theme.color.primary};
    ${is('disabled')`
      color: ${({ theme }) => theme.color.grey200};
    `}
    ${isNot('disabled')`
      &:hover {
        background-color: ${({ theme }) => theme.color.primary150};
        color: ${({ theme }) => theme.color.white};
      }
    `}
  }
`;

export const ButtonPrimaryFocused = styled(ButtonPrimary)`
  &&& {
    color: ${({ theme }) => theme.color.red200};
    background-color: ${({ theme }) => theme.color.primary400};
    font-weight: ${({ theme }) => theme.fontWeight.semiBold};
    font-size: ${({ theme }) => theme.fontSize.normal};
    &:hover {
      background-color: ${({ theme }) => theme.color.primary200};
      color: ${({ theme }) => theme.color.red200};
    }
    svg {
      path {
        fill: ${({ theme }) => theme.color.red200} !important;
      }
      height: 13.5px;
      width: 13.5px;
    }
  }
`;

export const ButtonSmall = styled(Button)`
&&&{
  background-color: ${props => (props.pink ? ({ theme }) => theme.color.primary150 : ({ theme }) => theme.color.white)};
  color: ${props => (props.pink ? ({ theme }) => theme.color.white : ({ theme }) => theme.color.primary150)};
  height: 36px;
  border-radius: ${props => (props.radius) ? props.radius : '40px'};
  font-size: ${({ theme }) => theme.fontSize.md};
  border: 1px solid ${({ theme }) => theme.color.primary150};
  line-height: 22px;
  width: ${props => (props.width)};;
}
`;
